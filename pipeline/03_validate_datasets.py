#!/usr/bin/env python3
"""
Step 03: Prépare un dataset économétrique par période pour l'analyse R.
Ce script fusionne les exports et désastres pour chaque période, garde les colonnes utiles, et sauvegarde un CSV par période sous le nom
datasets/econometric_dataset_<period>.csv. Plus de dataset unique.
"""

import sys
from pathlib import Path
import pandas as pd
from loguru import logger
import json
import numpy as np
from utils.utils import clean_iso_codes

# Load config from config.json
CONFIG_PATH = Path(__file__).parent / "config.json"
with open(CONFIG_PATH, "r") as f:
    config = json.load(f)

CACHE_DIR = Path(config["CACHE_DIR"])
DATASETS_DIR = Path(config["DATASETS_DIR"])
EXPORT_PERIODS = [tuple(period) for period in config["EXPORT_PERIODS"]]
LOG_LEVEL = config["LOG_LEVEL"]

logger.remove()
logger.add(sys.stderr, level=LOG_LEVEL)

periods = {f"{start}_{end}": (start, end) for (start, end) in EXPORT_PERIODS}

print("\n==================================\n    ✅ VALIDATE DATASETS (3/4)   \n==================================\n")

any_success = False
for period, (start, end) in periods.items():
    try:
        # --- Load exports ---
        exports_cache_file = CACHE_DIR / "exports_combined.pkl"
        if not exports_cache_file.exists():
            logger.error(f"Export cache file not found: {exports_cache_file}")
            continue
        exports_data = pd.read_pickle(exports_cache_file)
        period_key = f"{start}_{end}"
        if period_key not in exports_data:
            logger.error(f"Period {period_key} not found in export cache")
            continue
        exports = exports_data[period_key].copy()
        # Nettoyage et exclusion des codes ISO après chargement exports
        excluded_iso_codes = config.get("EXCLUDED_ISO_CODES", [])
        exports = clean_iso_codes(exports, iso_col="ISO", exclude_iso_codes=excluded_iso_codes)
        # --- Load disasters ---
        disasters_cache_file = CACHE_DIR / f"disasters_combined_{start}_{end}.pkl"
        if not disasters_cache_file.exists():
            logger.error(f"Disaster cache file not found: {disasters_cache_file}")
            continue
        disasters = pd.read_pickle(disasters_cache_file)
        # Nettoyage et exclusion des codes ISO après chargement disasters
        disasters = clean_iso_codes(disasters, iso_col="ISO", exclude_iso_codes=excluded_iso_codes)
        # --- Merge at product-country-year level ---
        merged = exports.merge(disasters, on=["ISO", "Year"], how="inner")
        # Nettoyage et exclusion des codes ISO après merge
        merged = clean_iso_codes(merged, iso_col="ISO", exclude_iso_codes=excluded_iso_codes)
        # --- Correction: restaurer la colonne Country si perdue lors du merge ---
        if "Country" not in merged.columns:
            if "Country" in exports.columns:
                merged["Country"] = merged["ISO"].map(dict(zip(exports["ISO"], exports["Country"])))
                logger.info("[MERGE] Colonne 'Country' restaurée à partir des exports après merge.")
            else:
                logger.error("Colonne 'Country' absente des exports et du merge. Impossible de continuer.")
                raise ValueError("Colonne 'Country' absente des exports et du merge. Impossible de continuer.")
        # --- Correction: laisser is_agri en booléen natif (True/False) pour compatibilité R ---
        # (plus de conversion en 1/0 ici)
        # --- Dynamically detect disaster variables, significant event flags, and controls ---
        base_cols = [
            "ISO", "Country", "Year", "cmdCode", "fobvalue", "is_agri"
        ]
        # Disaster variables: deaths, affected, events, intensity, index
        disaster_cols = [
            col for col in merged.columns if any(
                kw in col for kw in ["deaths", "affected", "events", "intensity", "index"]
            )
        ]
        # Significant event flags: *_sig_*, *_geomet_sig_*
        sig_flag_cols = [col for col in merged.columns if "_sig_" in col]
        # Controls: population, income, poverty, etc.
        control_cols = [
            col for col in ["is_poor_country", "is_small_country", "Population", "Income group"] if col in merged.columns
        ]
        # Combine all columns, preserving order and uniqueness
        keep_cols = base_cols + disaster_cols + sig_flag_cols + control_cols
        keep_cols = [col for i, col in enumerate(keep_cols) if col in merged.columns and col not in keep_cols[:i]]
        merged_final = merged[keep_cols].copy()
        # --- Contrôle strict des colonnes World Bank ---
        required_wb_cols = ['is_poor_country', 'is_small_country', 'Income group', 'Population']
        missing_wb_cols = [col for col in required_wb_cols if col not in merged_final.columns]
        if missing_wb_cols:
            logger.error(f"Colonnes World Bank manquantes dans le dataset économétrique final: {missing_wb_cols}")
            raise ValueError(f"Colonnes World Bank manquantes dans le dataset économétrique final: {missing_wb_cols}. Vérifiez la préparation et le merge des données sources.")
        # --- Forcer la présence de is_agri et réordonner les colonnes ---
        required_cols = ["Year", "Country", "ISO"]
        # Vérifier présence obligatoire de is_agri
        if "is_agri" not in merged_final.columns:
            logger.error("Colonne is_agri manquante dans le dataset final. Vérifiez la préparation des exports.")
            raise ValueError("Colonne is_agri manquante dans le dataset final. Vérifiez la préparation des exports.")
        other_cols = [col for col in merged_final.columns if col not in required_cols]
        ordered_cols = [col for col in required_cols if col in merged_final.columns] + other_cols
        merged_final = merged_final[ordered_cols]
        # 1. ln_total_occurrence : log(1 + somme des événements majeurs)
        event_cols = [col for col in merged_final.columns if col.endswith('_events')]
        if event_cols:
            merged_final['sum_events'] = merged_final[event_cols].sum(axis=1, skipna=True)
            merged_final['ln_total_occurrence'] = np.log1p(merged_final['sum_events'])
        # 2. ln_total_deaths : log(1 + somme des morts toutes catastrophes)
        death_cols = [col for col in merged_final.columns if col.endswith('_deaths')]
        if death_cols:
            merged_final['sum_deaths'] = merged_final[death_cols].sum(axis=1, skipna=True)
            merged_final['ln_total_deaths'] = np.log1p(merged_final['sum_deaths'])
        # 3. Variables log par type de catastrophe (ex: ln_earthquake_count)
        for dtype in config['DISASTER_TYPES']:
            dtype_key = dtype.lower().replace(' ', '_')
            count_col = f"{dtype_key}_events"
            log_col = f"ln_{dtype_key}_count"
            if count_col in merged_final.columns:
                merged_final[log_col] = np.log1p(merged_final[count_col])
        # 4. Classification revenu simplifiée (income_group_internal)
        if 'Income group' in merged_final.columns:
            merged_final['income_group_internal'] = np.where(
                merged_final['Income group'].isin(['High income', 'Upper middle income']), 'High', 'Low')
        # 5. Classification taille simplifiée (size_group)
        if 'Population' in merged_final.columns:
            pop_median = merged_final['Population'].median(skipna=True)
            merged_final['size_group'] = np.where(merged_final['Population'] > pop_median, 'Large', 'Small')
        # 6. d_ln_population : diff du log population par pays-année
        if 'Population' in merged_final.columns:
            merged_final = merged_final.sort_values(['ISO', 'cmdCode', 'Year'])
            merged_final['ln_population'] = np.log(merged_final['Population'])
            merged_final['d_ln_population'] = merged_final.groupby(['ISO', 'cmdCode'])['ln_population'].diff()
        # 7. disaster_index : somme pondérée des intensités normalisées (GeoMet)
        intensity_cols = [col for col in merged_final.columns if col.endswith('_intensity')]
        if intensity_cols:
            merged_final['disaster_index'] = merged_final[intensity_cols].sum(axis=1, skipna=True)
        # --- Logging summary ---
        n_obs = len(merged_final)
        n_iso = merged_final['ISO'].nunique() if 'ISO' in merged_final.columns else 'N/A'
        n_prod = merged_final['cmdCode'].nunique() if 'cmdCode' in merged_final.columns else 'N/A'
        an_min = merged_final['Year'].min() if 'Year' in merged_final.columns else 'N/A'
        an_max = merged_final['Year'].max() if 'Year' in merged_final.columns else 'N/A'
        n_agri = merged_final['is_agri'].sum() if 'is_agri' in merged_final.columns else 0

        # Warnings if unexpected NA or 0 values
        if n_obs == 0:
            logger.warning(f"Nombre d'observations nul pour la période {start}-{end}.")
        if n_iso == 'N/A' or n_iso == 0:
            logger.warning(f"Nombre de pays (ISO) nul ou non disponible pour la période {start}-{end}.")
        if n_prod == 'N/A' or n_prod == 0:
            logger.warning(f"Nombre de produits (cmdCode) nul ou non disponible pour la période {start}-{end}.")
        if an_min == 'N/A' or pd.isna(an_min):
            logger.warning(f"Année minimale (Year) non disponible pour la période {start}-{end}.")
        if an_max == 'N/A' or pd.isna(an_max):
            logger.warning(f"Année maximale (Year) non disponible pour la période {start}-{end}.")
        if n_agri == 0:
            logger.warning(f"Aucune observation agricole (is_agri=1) pour la période {start}-{end}.")
        pct_agri = (n_agri / n_obs * 100) if n_obs > 0 else 0
        # Log nombre de small/poor countries pour l'année de référence
        ref_year = config.get("POOR_COUNTRY_YEAR", 2016)
        n_small = merged_final.query(f"Year == {ref_year} and is_small_country == 1")["ISO"].nunique() if "is_small_country" in merged_final.columns else 'N/A'
        n_poor = merged_final.query(f"Year == {ref_year} and is_poor_country == 1")["ISO"].nunique() if "is_poor_country" in merged_final.columns else 'N/A'
        logger.info(
            f"\n📊 ECONOMETRIC DATASET {start}-{end}\n"
            f"  • Observations : {n_obs:,}\n"
            f"  • Years        : {an_min}–{an_max}\n"
            f"  • Countries    : {n_iso}\n"
            f"  • Products     : {n_prod}\n"
            f"  • % agri       : {pct_agri:.1f}%\n"
            f"  • Small countries ({ref_year}) : {n_small}\n"
            f"  • Poor countries  ({ref_year}) : {n_poor}\n"
        )
        logger.trace(f"Columns:\n{list(merged_final.columns)}\n")
        # --- Diagnostic croisé pour NA R : présence des combinaisons critiques ---
        # Pour chaque type de catastrophe, compter les lignes *_sig_* == 1 par sous-groupe
        disaster_types = config.get('DISASTER_TYPES', [])
        sig_flags = [col for col in merged_final.columns if col.endswith('_sig_p90') or col.endswith('_sig_anydeaths') or col.endswith('_sig_abs1000')]
        for dtype in disaster_types:
            dtype_key = dtype.lower().replace(' ', '_')
            for flag in ['sig_p90', 'sig_anydeaths', 'sig_abs1000']:
                colname = f"{dtype_key}_{flag}"
                if colname in merged_final.columns:
                    for group, group_label in [("is_poor_country", "Poor"), ("is_small_country", "Small")]:
                        if group in merged_final.columns:
                            n = merged_final.query(f"{group} == 1 and {colname} == 1").shape[0]
                            logger.debug(f"[DIAG] {colname} & {group}=1 : {n} lignes")
        # --- Save ---
        DATASETS_DIR.mkdir(exist_ok=True)
        out_csv = DATASETS_DIR / f"econometric_dataset_{start}_{end}.csv"
        merged_final.to_csv(out_csv, index=False)
        logger.info(f"✅ Saved econometric dataset: {out_csv.name} ({n_obs:,} rows)")
        any_success = True
    except Exception as e:
        logger.error(f"Error during dataset preparation for period {period}: {e}")
if not any_success:
    logger.error("❌ Aucune période n'a pu être traitée correctement.\n")
    sys.exit(1)