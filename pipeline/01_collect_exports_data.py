#!/usr/bin/env python3
"""
Pipeline indépendante de collecte des données d'exportation depuis Comtrade.

- Aucune dépendance legacy (pas de get_exports_dataframe)
- Utilise uniquement les périodes et paramètres de config.py
- Boucle sur EXPORT_PERIODS importé
- Ne crée ni ne renomme aucune colonne (on garde fobvalue, cmdCode, etc. tels quels)
- Aucun calcul de log ou transformation
- Validation minimale (colonnes critiques, valeurs positives)
- Logging simple et clair
"""

import sys
import os
import pandas as pd
from pathlib import Path
from loguru import logger
import pickle
import json

# Permet d'importer toolkit même si on lance depuis pipeline/
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Load config from config.json
CONFIG_PATH = Path(__file__).parent / "config.json"
with open(CONFIG_PATH, "r") as f:
    config = json.load(f)

CACHE_DIR = Path(config["CACHE_DIR"])
DATA_DIR = Path(config["DATA_DIR"])
EXPORT_PERIODS = [tuple(period) for period in config["EXPORT_PERIODS"]]
CLEAR_CACHE = config["CLEAR_CACHE"]
LOG_LEVEL = config["LOG_LEVEL"]

from utils.utils import fetch_comtrade_exports, clean_iso_codes

logger.remove()
logger.add(sys.stderr, level=LOG_LEVEL)

# Créer les dossiers nécessaires
CACHE_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)
(DATA_DIR / "exports").mkdir(parents=True, exist_ok=True)
Path(config.get("DATASETS_DIR", "datasets")).mkdir(parents=True, exist_ok=True)
Path(config.get("RESULTS_DIR", "results")).mkdir(parents=True, exist_ok=True)
Path(config.get("TABLES_DIR", "results/tables")).mkdir(parents=True, exist_ok=True)


def get_cache_path(data_type: str) -> Path:
    return CACHE_DIR / f"{data_type}.pkl"


def load_exports_from_csv(year_range: tuple) -> pd.DataFrame:
    import re
    start_year, end_year = year_range
    logger.info(f"Chargement exports CSV pour {start_year}-{end_year}")
    exports_dir = DATA_DIR / "exports"
    all_exports = []
    years_found = set()
    # Recherche tous les fichiers CSV pertinents
    csv_files = list(exports_dir.glob("*_exports*.csv"))
    file_years_map = {}
    for csv_file in csv_files:
        fname = csv_file.name
        # Plage d'années : 1979-1987_exports_...
        m_range = re.match(r"(\d{4})-(\d{4})_exports", fname)
        if m_range:
            y0, y1 = int(m_range.group(1)), int(m_range.group(2))
            covered = set(range(y0, y1 + 1))
        else:
            # Année simple : 1988_exports_...
            m_single = re.match(r"(\d{4})_exports", fname)
            if m_single:
                y0 = int(m_single.group(1))
                covered = {y0}
            else:
                continue
        file_years_map[csv_file] = covered
    # Sélectionne les fichiers couvrant la période demandée
    target_years = set(range(start_year, end_year + 1))
    files_to_load = []
    for f, covered in file_years_map.items():
        if covered & target_years:
            files_to_load.append(f)
            years_found |= (covered & target_years)
    # Chargement des fichiers
    for f in files_to_load:
        try:
            try:
                df = pd.read_csv(f)
            except UnicodeDecodeError:
                df = pd.read_csv(f, encoding="latin1")
            all_exports.append(df)
        except Exception as e:
            logger.warning(f"Erreur lecture {f}: {e}")
    if not all_exports:
        logger.error(f"Aucune donnée d'exports trouvée pour {start_year}-{end_year}")
        return pd.DataFrame()
    df = pd.concat(all_exports, ignore_index=True)
    # Harmonisation des colonnes (mapping direct)
    df = df.rename(columns={
        "refYear": "Year",
        "reporterISO": "ISO",
        "reporterDesc": "Country"
    })
    # Nettoyage et exclusion des codes ISO
    excluded_iso_codes = config.get("EXCLUDED_ISO_CODES", [])
    df = clean_iso_codes(df, iso_col="ISO", exclude_iso_codes=excluded_iso_codes)
    # Ajout colonne is_agri selon la nomenclature
    S2_AGRI_CODES = [
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
        21, 22, 23, 24, 25, 29,
        41, 42, 43
    ]
    H0_AGRI_CODES = [f"{i:02}" for i in range(1, 25)]
    def is_agri_row(row):
        classif = str(row["classificationSearchCode"]).strip().upper().replace("SITC2", "S2")
        if classif == "S2":
            try:
                code = int(str(row["cmdCode"]).strip())
            except Exception:
                logger.debug(f"cmdCode non convertible en int: {row['cmdCode']}")
                return False
            if code in S2_AGRI_CODES:
                logger.trace(f"AGRI S2: {row['Country']} {row['ISO']} {row['Year']} cmdCode={code} => True")
            return code in S2_AGRI_CODES
        elif classif == "HS":
            code = str(row["cmdCode"]).zfill(2)
            return code in H0_AGRI_CODES
        else:
            logger.warning(f"classificationSearchCode inattendu: {classif}")
            return False
    df["is_agri"] = df.apply(is_agri_row, axis=1)
    logger.debug(f"Exports CSV chargées: {len(df)} lignes pour {start_year}-{end_year}")
    return df

key_columns = ["Year", "Country", "ISO", "classificationCode", "classificationSearchCode", "cmdCode", "is_agri", "fobvalue"]
def validate_exports_data(df: pd.DataFrame) -> pd.DataFrame:
    # Vérifier colonnes critiques harmonisées
    for col in key_columns:
        if col not in df.columns:
            logger.error(f"Colonne manquante: {col}")
            return pd.DataFrame()
    df = df.dropna(subset=key_columns)
    df = df[df["fobvalue"] > 0]
    return df


def get_pipeline_options():
    import argparse
    parser = argparse.ArgumentParser(description="Exports data pipeline options")
    parser.add_argument("--clear-cache", dest="clear_cache", action="store_true", help="Clear cache before running (force rebuild)")
    parser.add_argument("--fetch-missing", dest="fetch_missing", action="store_true", help="Download missing years from Comtrade API if needed")
    parser.set_defaults(clear_cache=CLEAR_CACHE, fetch_missing=False)
    args, _ = parser.parse_known_args()
    return args.clear_cache, args.fetch_missing


def collect_exports_data(clear_cache: bool = False) -> dict:
    cache_file = get_cache_path("exports_combined")
    # Ne supprime que le cache d'exports, pas tout le dossier
    if clear_cache and cache_file.exists():
        cache_file.unlink()
        logger.info("Cache exports_combined.pkl supprimé avant exécution (option clear_cache)")
    if not clear_cache and cache_file.exists():
        logger.debug("Cache trouvé, chargement des données d'exportation")
        try:
            with open(cache_file, "rb") as f:
                cached_results = pickle.load(f)
            # Affichage résumé et aperçu pour chaque période du cache
            for period_name, df in cached_results.items():
                n_obs = len(df)
                an_min = df['Year'].min() if 'Year' in df.columns else 'N/A'
                an_max = df['Year'].max() if 'Year' in df.columns else 'N/A'
                n_iso = df['ISO'].nunique() if 'ISO' in df.columns else 'N/A'
                n_prod = df['cmdCode'].nunique() if 'cmdCode' in df.columns else 'N/A'
                logger.info(
                    f"\n📊 RÉSUMÉ EXPORTS {period_name}\n"
                    f"  • 📦 Observations : {n_obs:,}\n"
                    f"  • 📅 Années       : {an_min}–{an_max}\n"
                    f"  • 🌍 Pays         : {n_iso}\n"
                    f"  • 🏷️ Produits     : {n_prod}\n"
                )
                df = df.sort_values(["Year", "Country"]).reset_index(drop=True)
                preview_idx = [0] if len(df) > 0 else []
                if len(df) > 4:
                    import numpy as np
                    random_idx = np.random.choice(range(1, len(df)-1), size=3, replace=False)
                    preview_idx += list(random_idx)
                elif len(df) > 1:
                    preview_idx += list(range(1, len(df)-1))
                if len(df) > 1:
                    preview_idx.append(len(df)-1)
                preview_idx = sorted(set(preview_idx))
                preview_df = df.iloc[preview_idx][key_columns]
                logger.debug(f"\nAperçu (trié, 1ère, 3 random, dernière) :\n{preview_df.to_string(index=False)}")
            return cached_results
        except Exception as e:
            logger.warning(f"Erreur lecture cache: {e}")
    results = {}
    for period_idx, (start, end) in enumerate(EXPORT_PERIODS):
        period_name = f"{start}_{end}"
        logger.info(f"[1/4] Période {period_name} : collecte des exports...")
        df = load_exports_from_csv((start, end))
        if df.empty:
            logger.warning(f"Aucune donnée d'exports pour {period_name}")
            continue
        df = validate_exports_data(df)
        if df.empty:
            logger.warning(f"Données exports invalides pour {period_name}")
            continue
        # Regroupement des infos principales en un seul logger
        n_obs = len(df)
        an_min = df['Year'].min() if 'Year' in df.columns else 'N/A'
        an_max = df['Year'].max() if 'Year' in df.columns else 'N/A'
        n_iso = df['ISO'].nunique() if 'ISO' in df.columns else 'N/A'
        n_prod = df['cmdCode'].nunique() if 'cmdCode' in df.columns else 'N/A'
        # Logger résumé façon tableau avec smileys
        logger.info(
            f"\n📊 RÉSUMÉ EXPORTS {start}-{end}\n"
            f"  • 📦 Observations : {n_obs:,}\n"
            f"  • 📅 Années       : {an_min}–{an_max}\n"
            f"  • 🌍 Pays         : {n_iso}\n"
            f"  • 🏷️ Produits     : {n_prod}\n"
        )
        # Tri par Year puis Country
        df = df.sort_values(["Year", "Country"]).reset_index(drop=True)
        # Aperçu lisible : 1ère ligne, 3 random, dernière (colonnes clés)
        preview_idx = [0] if len(df) > 0 else []
        if len(df) > 4:
            import numpy as np
            random_idx = np.random.choice(range(1, len(df)-1), size=3, replace=False)
            preview_idx += list(random_idx)
        elif len(df) > 1:
            preview_idx += list(range(1, len(df)-1))
        if len(df) > 1:
            preview_idx.append(len(df)-1)
        preview_idx = sorted(set(preview_idx))
        preview_df = df.iloc[preview_idx][key_columns]
        logger.debug(f"\nAperçu (trié, 1ère, 3 random, dernière) :\n{preview_df.to_string(index=False)}")
        results[period_name] = df
    if results:
        logger.info("Sauvegarde cache des données d'exportation")
        try:
            with open(cache_file, "wb") as f:
                pickle.dump(results, f)
        except Exception as e:
            logger.warning(f"Erreur sauvegarde cache: {e}")
    logger.info("✅ Collecte des données d'exportation terminée")
    return results


if __name__ == "__main__":
    print(f"\n==================================\n   🚢 COLLECT EXPORTS DATA (1/4)   \n==================================\n")
    logger.remove()
    logger.add(sys.stderr, level="DEBUG")
    clear_cache, fetch_missing = get_pipeline_options()
    from utils.utils import fetch_comtrade_exports
    exports_dir = DATA_DIR / "exports"
    exports_dir.mkdir(parents=True, exist_ok=True)
    for (start, end) in EXPORT_PERIODS:
        if fetch_missing:
            logger.info(f"Récupération des données manquantes sur {start}-{end} via Comtrade API...")
            fetch_comtrade_exports(
                output_path=str(exports_dir),
                year_start=start,
                year_end=end,
                replace=False
            )
    data = collect_exports_data(clear_cache=clear_cache)
    any_success = False
    for period, df in data.items():
        if not df.empty:
            any_success = True
    if any_success:
        logger.info(f"✅ Données exports sauvegardées dans le cache fichier '{get_cache_path('exports_combined')}'.\n")
    else:
        logger.error("❌ Aucune période n'a pu être traitée correctement.\n")
        sys.exit(1)
