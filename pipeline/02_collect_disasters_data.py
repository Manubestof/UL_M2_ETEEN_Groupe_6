#!/usr/bin/env python3
"""
Natural disaster data collection and preparation module.
Combines EM-DAT and GeoMet according to the reference article's methodology.
"""

import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
from loguru import logger
import json
from utils.utils import clean_iso_codes

# Load config from config.json
CONFIG_PATH = Path(__file__).parent / "config.json"
with open(CONFIG_PATH, "r") as f:
    config = json.load(f)

CACHE_DIR = Path(config["CACHE_DIR"])
DATA_DIR = Path(config["DATA_DIR"])
LOG_LEVEL = config["LOG_LEVEL"]
EXCLUDED_ISO_CODES = config["EXCLUDED_ISO_CODES"]
DISASTER_TYPES = config["DISASTER_TYPES"]
EXPORT_PERIODS = [tuple(period) for period in config["EXPORT_PERIODS"]]

# Configure logging
logger.remove()
logger.add(sys.stderr, level=LOG_LEVEL)

# Define data subdirectories from DATA_DIR (already imported)
EMDAT_DIR = DATA_DIR / "emdat"
GEOMET_DIR = DATA_DIR / "geomet"
WORLDBANK_DIR = DATA_DIR / "world_bank"
UNDESA_DIR = DATA_DIR / "undesa"

# Create necessary directories
for dir_path in [CACHE_DIR, EMDAT_DIR, GEOMET_DIR, WORLDBANK_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Centralized pipeline options (CLI > config.py)
def get_pipeline_options():
    import argparse
    parser = argparse.ArgumentParser(description="Disaster data pipeline options")
    parser.add_argument("--clear-cache", dest="clear_cache", action="store_true", help="Clear cache before running (force rebuild)")
    parser.set_defaults(clear_cache=False)
    args, _ = parser.parse_known_args()
    return args.clear_cache

# DataFrame preview (sorted, first, 3 random, last)
def preview_dataframe(df, key_columns=None, n_random=3):
    if df.empty:
        logger.warning("No preview possible: DataFrame is empty.")
        return
    # Only keep a subset of important columns for preview
    default_cols = [
        col for col in ["Year", "Country", "ISO"] if col in df.columns
    ]
    # Dynamically build disaster_cols from DISASTER_TYPES in config
    disaster_cols = []
    for dtype in DISASTER_TYPES:
        base = dtype.lower().replace(" ", "_")
        for suffix in ["deaths", "affected", "events"]:
            colname = f"{base}_{suffix}"
            if colname in df.columns:
                disaster_cols.append(colname)
    # Add any total columns if present
    for col in ["total_deaths", "total_events"]:
        if col in df.columns:
            disaster_cols.append(col)
    preview_cols = default_cols + disaster_cols
    if not preview_cols:
        preview_cols = df.columns[:min(5, len(df.columns))].tolist()
    df = df.sort_values([c for c in ["Year", "Country"] if c in df.columns]).reset_index(drop=True)
    preview_idx = [0] if len(df) > 0 else []
    if len(df) > 4:
        random_idx = np.random.choice(range(1, len(df)-1), size=min(n_random, len(df)-2), replace=False)
        preview_idx += list(random_idx)
    elif len(df) > 1:
        preview_idx += list(range(1, len(df)-1))
    if len(df) > 1:
        preview_idx.append(len(df)-1)
    preview_idx = sorted(set(preview_idx))
    preview_df = df.iloc[preview_idx][preview_cols] if preview_cols else df.iloc[preview_idx]
    logger.trace(f"\nPreview (sorted, first, {n_random} random, last):\n{preview_df.to_string(index=False)}")

def load_emdat_data(year_start: int, year_end: int) -> pd.DataFrame:
    """
    Load EM-DAT data for the specified period.

    Args:
        year_start: Start year
        year_end: End year

    Returns:
        DataFrame with EM-DAT data
    """
    logger.info(f"[EM-DAT] Début du chargement pour la période {year_start}-{year_end}")
    try:
        # Prétraitement spécifique selon la période
        if year_end <= 2000:
            emdat_file = EMDAT_DIR / "EM-DAT 1979-2000.xlsx"
            logger.info("[EM-DAT] Fichier source : EM-DAT 1979-2000.xlsx (structure événement par ligne, non agrégée)")
            if not emdat_file.exists():
                logger.error(f"EM-DAT file not found: {emdat_file}")
                return pd.DataFrame()
            df = pd.read_excel(emdat_file, sheet_name="EM-DAT Data")
            logger.debug("[EM-DAT] Lecture de la feuille 'EM-DAT Data'")
        else:
            emdat_file = EMDAT_DIR / "EM-DAT countries 2000+.xlsx"
            logger.info("[EM-DAT] Fichier source : EM-DAT countries 2000+.xlsx (structure déjà agrégée par pays-année)")
            if not emdat_file.exists():
                logger.error(f"EM-DAT file not found: {emdat_file}")
                return pd.DataFrame()
            df = pd.read_excel(emdat_file, skiprows=[1])
            logger.debug("[EM-DAT] Lecture avec skiprows=[1]")
        logger.info(f"[EM-DAT] {len(df):,} lignes chargées depuis {emdat_file.name}")
        logger.trace(f"[EM-DAT] Colonnes disponibles : {list(df.columns)}")
        # Harmonisation année
        if year_end <= 2000:
            if "Start Year" in df.columns:
                df["Start Year"] = pd.to_numeric(df["Start Year"], errors="coerce")
                df = df.dropna(subset=["Start Year"])
                df = df[(df["Start Year"] >= year_start) & (df["Start Year"] <= year_end)]
                df["Year"] = df["Start Year"].astype(int)
                logger.info(f"[EM-DAT] Filtrage sur la période {year_start}-{year_end} : {len(df):,} lignes conservées")
                logger.info("[EM-DAT] Colonne 'Year' créée à partir de 'Start Year' pour harmonisation")
            else:
                logger.warning("[EM-DAT] Colonne 'Start Year' absente dans le fichier 1979-2000")
        else:
            if "Year" in df.columns:
                df = df[df["Year"] != "#date +occurred"]
                df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
                df = df.dropna(subset=["Year"])
                df["Year"] = df["Year"].astype(int)
                df = df[(df["Year"] >= year_start) & (df["Year"] <= year_end)]
                logger.info(f"[EM-DAT] Filtrage sur la période {year_start}-{year_end} : {len(df):,} lignes conservées")
            else:
                logger.warning("[EM-DAT] Colonne 'Year' absente dans le fichier 2000+")
        # After loading, normalize ISO codes
        if not df.empty and "ISO" in df.columns:
            df = clean_iso_codes(df, iso_col="ISO")
        return df
    except Exception as e:
        logger.error(f"[EM-DAT] Erreur lors du chargement : {e}")
        return pd.DataFrame()


def load_geomet_data(year_start: int, year_end: int) -> pd.DataFrame:
    """
    Load GeoMet data for the specified period.

    Args:
        year_start: Start year
        year_end: End year

    Returns:
        DataFrame with GeoMet data
    """
    logger.info(f"Loading GeoMet data for period {year_start}-{year_end}")

    geomet_file = GEOMET_DIR / "IfoGAME_EMDAT.dta"

    try:
        # Use Stata reader as in view_raw_data.py
        df = pd.read_stata(geomet_file)
        logger.info(f"Loaded GeoMet data: {len(df):,} records")

        # Filter by year range
        if "year" in df.columns:
            df = df[(df["year"] >= year_start) & (df["year"] <= year_end)]
            # Standardize column name
            df = df.rename(columns={"year": "Year", "iso": "ISO"})
            logger.info(
                f"Filtered to {len(df):,} records for period {year_start}-{year_end}"
            )
        else:
            logger.warning("No 'year' column found in GeoMet data")

        # Show available columns
        logger.trace(f"Available GeoMet columns: {list(df.columns)}")

        # After loading, normalize ISO codes
        if not df.empty and "ISO" in df.columns:
            df = clean_iso_codes(df, iso_col="ISO")

        return df

    except Exception as e:
        logger.error(f"Failed to load GeoMet data: {e}")
        return pd.DataFrame()

# --- AGGREGATE GEOMET INTENSITY BY COUNTRY-YEAR AND DISASTER TYPE ---
def aggregate_geomet_intensity(geomet_df):
    if geomet_df.empty:
        return geomet_df
    # Mapping from disaster type to GeoMet variable suffix
    type_suffixes = {
        'Earthquake': 'eq',
        'Flood': 'fld',
        'Storm': 'str',
        'Extreme temperature': 'temp',
    }
    # For each disaster type, aggregate proxies
    agg_frames = []
    for dtype, suffix in type_suffixes.items():
        # Find all relevant columns for this type
        proxies = [
            f'killed_pop_{suffix}',
            f'affected_pop_{suffix}',
            f'damage_gdp_{suffix}'
        ]
        cols_present = [c for c in proxies if c in geomet_df.columns]
        if not cols_present:
            continue
        # Sum across all proxies for intensity
        geomet_df[f'{dtype.lower().replace(" ", "_")}_intensity'] = geomet_df[cols_present].sum(axis=1, skipna=True)
        # Aggregate by ISO, Year
        agg = geomet_df.groupby(['ISO', 'Year'])[f'{dtype.lower().replace(" ", "_")}_intensity'].sum().reset_index()
        agg_frames.append(agg)
    # Merge all *_intensity columns into a single DataFrame
    if not agg_frames:
        return geomet_df
    intensity_df = agg_frames[0]
    for frame in agg_frames[1:]:
        intensity_df = intensity_df.merge(frame, on=['ISO', 'Year'], how='outer')
    # Merge back into original GeoMet (on ISO, Year)
    geomet_df = geomet_df.drop(columns=[c for c in geomet_df.columns if c.endswith('_intensity')], errors='ignore')
    geomet_df = geomet_df.merge(intensity_df, on=['ISO', 'Year'], how='left')
    return geomet_df


def load_income_and_population() -> pd.DataFrame:
    """
    Load World Bank data for country classification and population.

    Returns:
        DataFrame with country development indicators and population by ISO, Year
    """
    logger.info("Loading World Bank country classification and population data")

    try:
        # Load income classification
        income_file = WORLDBANK_DIR / "country_income_classification.xlsx"
        df_income = pd.read_excel(income_file)
        df_income = df_income.rename(columns={"Code": "ISO"})
        df_income["is_poor_country"] = df_income["Income group"].isin([
            "Low income", "Lower middle income"
        ])
        df_income = df_income[["ISO", "Income group", "is_poor_country"]]

        # Load population data (explicit mapping)
        pop_file = UNDESA_DIR / "total_population.xlsx"
        df_pop = pd.read_excel(pop_file, sheet_name="Estimates", header=16)
        df_pop = df_pop.rename(
            columns={
                "Region, subregion, country or area *": "Country",
                "ISO3 Alpha-code": "ISO",
                "Total Population, as of 1 January (thousands)": "Population"
            }
        )
        # Nettoyage population : on ne garde que les vraies lignes pays
        if "Type" in df_pop.columns:
            df_pop = df_pop[df_pop["Type"] == "Country/Area"]
        df_pop = df_pop.dropna(subset=["ISO"])
        df_pop = df_pop[~df_pop["ISO"].isin(EXCLUDED_ISO_CODES)]
        df_pop["Population"] = pd.to_numeric(df_pop["Population"], errors="coerce") * 1000
        # Nettoyer les NA dans Year avant conversion
        df_pop = df_pop[df_pop["Year"].notna()]
        df_pop["Year"] = pd.to_numeric(df_pop["Year"], errors="coerce")
        df_pop = df_pop[df_pop["Year"].notna()]
        df_pop["Year"] = df_pop["Year"].astype(int)
        df_pop = df_pop[["ISO", "Year", "Population"]]

        # Merge income and population on ISO only (no Country)
        df = df_pop.merge(df_income, on=["ISO"], how="left")
        logger.info(f"Loaded World Bank data: {len(df_income)} countries (income), {len(df_pop)} country-years (population)")
        # Filtrer les ISO exclus dans le DataFrame final (après merge)
        df = df[~df["ISO"].isin(EXCLUDED_ISO_CODES)]
        return df

    except Exception as e:
        logger.error(f"Failed to load World Bank data: {e}")
        return pd.DataFrame()


def process_emdat_disasters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process EM-DAT data to create disaster variables.

    Args:
        df: Raw EM-DAT DataFrame

    Returns:
        DataFrame with processed disaster variables
    """
    logger.info("Processing EM-DAT disaster data...")

    if df.empty:
        logger.warning("EM-DAT DataFrame is empty")
        return df

    # Harmonize column names
    if "ISO" not in df.columns and "reporterISO" in df.columns:
        df = df.rename(columns={"reporterISO": "ISO"})
    if "Country" not in df.columns and "reporterDesc" in df.columns:
        df = df.rename(columns={"reporterDesc": "Country"})
    if "Year" not in df.columns and "refYear" in df.columns:
        df = df.rename(columns={"refYear": "Year"})

    # Exclude obsolete/dependent countries
    df = df[~df["ISO"].isin(EXCLUDED_ISO_CODES)]

    # Ensure we work on a copy to avoid SettingWithCopyWarning
    df = df.copy()

    # Required columns (adapt as needed)
    required_cols = [
        "ISO",
        "Country",
        "Year",
        "Disaster Type",
        "Total Deaths",
        "Total Affected",
    ]
    missing_cols = [col for col in required_cols if col not in df.columns]

    if missing_cols:
        logger.warning(f"Missing columns in EM-DAT data: {missing_cols}")
        # Create missing columns with default values
        for col in missing_cols:
            if col == "Total Deaths":
                df[col] = 0
            elif col == "Total Affected":
                df[col] = 0
            elif col == "Disaster Type":
                df[col] = "Unknown"
            elif col == "Year" and "Start Year" in df.columns:
                df[col] = df["Start Year"]

    # Clean and standardize disaster types
    disaster_mapping = {
        "Earthquake": "Earthquake",
        "Flood": "Flood",
        "Storm": "Storm",
        "Extreme temperature": "Extreme temperature",
        "Drought": "Extreme temperature",
        "Heat wave": "Extreme temperature",
        "Cold wave": "Extreme temperature",
    }

    # Correction SettingWithCopyWarning: ensure .copy() before assignment
    df = df[df["Disaster Type"].isin(DISASTER_TYPES)].copy()
    df["Disaster Type"] = df["Disaster Type"].map(disaster_mapping).fillna("Other")
    logger.info(f"Filtered to {len(df):,} records for disaster types of interest")

    # Create aggregated variables by country-year-type
    disaster_vars = []

    for disaster_type in DISASTER_TYPES:
        type_df = df[df["Disaster Type"] == disaster_type].copy()

        # Aggregate by country-year
        agg_df = (
            type_df.groupby(["ISO", "Country", "Year"])
            .agg(
                {
                    "Total Deaths": "sum",
                    "Total Affected": "sum",
                    "Disaster Type": "count",  # Number of events
                }
            )
            .reset_index()
        )

        # Rename columns
        disaster_name = disaster_type.lower().replace(" ", "_")
        agg_df = agg_df.rename(
            columns={
                "Total Deaths": f"{disaster_name}_deaths",
                "Total Affected": f"{disaster_name}_affected",
                "Disaster Type": f"{disaster_name}_events",
            }
        )

        disaster_vars.append(agg_df)

    # Merge all disaster variables
    if disaster_vars:
        result = disaster_vars[0]
        for var_df in disaster_vars[1:]:
            result = result.merge(var_df, on=["ISO", "Country", "Year"], how="outer")

        # Fill missing values with 0
        disaster_cols = [
            col
            for col in result.columns
            if any(d in col for d in ["deaths", "affected", "events"])
        ]
        result[disaster_cols] = result[disaster_cols].fillna(0)

        logger.info(
            f"Processed EM-DAT data: {len(result):,} country-year observations"
        )
        logger.trace(f"Created variables: {disaster_cols}")

        return result

    else:
        logger.warning("No disaster data to process")
        return pd.DataFrame()


def create_disaster_dataset(
    year_start: int, year_end: int, clear_cache: bool = False
) -> pd.DataFrame:
    """
    Create the complete disaster dataset by combining EM-DAT and GeoMet.
    """
    cache_file = CACHE_DIR / f"disasters_combined_{year_start}_{year_end}.pkl"

    # Cache management (clear) : ne supprime que le cache de la période concernée
    if clear_cache and cache_file.exists():
        cache_file.unlink()
        logger.info(f"Cache deleted: {cache_file}")

    # Always use cache if present (unless clear_cache)
    if not clear_cache and cache_file.exists():
        logger.info(f"Loading disasters from cache: {cache_file}")
        try:
            df = pd.read_pickle(cache_file)
            logger.info(f"{len(df):,} observations loaded from cache")
            # Summary and preview
            n_obs = len(df)
            an_min = df['Year'].min() if 'Year' in df.columns else 'N/A'
            an_max = df['Year'].max() if 'Year' in df.columns else 'N/A'
            n_iso = df['ISO'].nunique() if 'ISO' in df.columns else 'N/A'
            logger.info(
                f"\n📊 DISASTER SUMMARY {year_start}-{year_end}\n"
                f"  • Observations : {n_obs:,}\n"
                f"  • Years        : {an_min}–{an_max}\n"
                f"  • Countries    : {n_iso}\n"
            )
            preview_dataframe(df)
            return df
        except Exception as e:
            logger.warning(f"Cache read error: {e}")

    logger.info(f"Creating disaster dataset for {year_start}-{year_end}")

    # Load base data
    emdat_df = load_emdat_data(year_start, year_end)
    geomet_df = load_geomet_data(year_start, year_end)
    # AGGREGATE GEOMET INTENSITY COLUMNS
    geomet_df = aggregate_geomet_intensity(geomet_df)
    worldbank_df = load_income_and_population()

    # Process EM-DAT
    if not emdat_df.empty:
        emdat_processed = process_emdat_disasters(emdat_df)
    else:
        emdat_processed = pd.DataFrame()

    # Combine data sources
    if not emdat_processed.empty and not geomet_df.empty:
        combined_df = emdat_processed.merge(geomet_df, on=["ISO", "Year"], how="outer")
        logger.info("Merged EM-DAT and GeoMet")
    elif not emdat_processed.empty:
        combined_df = emdat_processed
        logger.warning("Only EM-DAT data used (GeoMet unavailable)")
    elif not geomet_df.empty:
        combined_df = geomet_df
        logger.warning("Only GeoMet data used (EM-DAT unavailable)")
    else:
        logger.error("No disaster data available")
        return pd.DataFrame()

    # Create full country-year panel
    countries = combined_df["ISO"].unique()
    years = range(year_start, year_end + 1)
    full_panel = pd.MultiIndex.from_product(
        [countries, years], names=["ISO", "Year"]
    ).to_frame(index=False)
    result = full_panel.merge(combined_df, on=["ISO", "Year"], how="left")

    # --- PANEL RESTRICTION: Only keep (ISO, Year) present in EM-DAT, GeoMet, or Exports, and present in World Bank population ---
    # Get valid (ISO, Year) from EM-DAT and GeoMet
    valid_iso_year = set()
    if not emdat_processed.empty:
        valid_iso_year.update(set(emdat_processed[["ISO", "Year"]].drop_duplicates().itertuples(index=False, name=None)))
    if not geomet_df.empty:
        valid_iso_year.update(set(geomet_df[["ISO", "Year"]].drop_duplicates().itertuples(index=False, name=None)))
    # Optionally: add exports (step 1) if available in cache
    exports_cache_file = CACHE_DIR / "exports_combined.pkl"
    if exports_cache_file.exists():
        try:
            exports_data = pd.read_pickle(exports_cache_file)
            for period_key, df_exp in exports_data.items():
                if isinstance(df_exp, pd.DataFrame):
                    valid_iso_year.update(set(df_exp[["ISO", "Year"]].drop_duplicates().itertuples(index=False, name=None)))
        except Exception as e:
            logger.warning(f"Could not load exports for panel restriction: {e}")
    # Restrict to (ISO, Year) present in World Bank population
    if not worldbank_df.empty:
        wb_iso_year = set(worldbank_df[["ISO", "Year"]].drop_duplicates().itertuples(index=False, name=None))
        valid_iso_year = valid_iso_year & wb_iso_year
    # Build panel only for valid (ISO, Year)
    panel_df = pd.DataFrame(list(valid_iso_year), columns=["ISO", "Year"])
    # --- Make combined_df unique on (ISO, Year) ---
    combined_df_unique = combined_df.drop_duplicates(subset=["ISO", "Year"])
    result = panel_df.merge(combined_df_unique, on=["ISO", "Year"], how="left")

    # Fill missing country info (use first non-null value per ISO)
    country_info_cols = ["ISO", "Country"]
    country_info = combined_df[country_info_cols].drop_duplicates(subset=["ISO"]).reset_index(drop=True)
    result = result.merge(country_info, on="ISO", how="left", suffixes=("", "_y"))
    if "Country_y" in result.columns:
        result["Country"] = result["Country"].fillna(result["Country_y"])
        result = result.drop(columns=["Country_y"])

    # Fill disaster variables with 0
    disaster_cols = [
        col for col in result.columns if any(term in col for term in ["deaths", "affected", "events", "intensity", "index"])
    ]
    result[disaster_cols] = result[disaster_cols].fillna(0)

    # Merge population and income (World Bank) on ISO, Year (une seule fois, toutes les colonnes)
    if not worldbank_df.empty:
        pop_cols = ["ISO", "Year", "Population", "is_poor_country", "Income group"]
        worldbank_df = worldbank_df[pop_cols]
        # Ajout du booléen is_small_country par année selon le seuil du config
        if "Population" in worldbank_df.columns:
            worldbank_df["is_small_country"] = (worldbank_df["Population"] < config["SMALL_COUNTRY_THRESHOLD"]).astype(int)
        result = result.merge(worldbank_df, on=["ISO", "Year"], how="left")
        logger.info("Added World Bank population and income data (ISO, Year) + is_small_country")

    # Fill missing population with 0 (or np.nan if you prefer)
    if "Population" in result.columns:
        result["Population"] = result["Population"].fillna(0)

    # --- SIGNIFICANT EVENT FLAGS ---
    # For each disaster type, create several boolean flags for significant events (EM-DAT)
    for dtype in DISASTER_TYPES:
        base = dtype.lower().replace(" ", "_")
        deaths_col = f"{base}_deaths"
        flag_median = []
        flag_p90 = []
        flag_abs = []
        flag_any = []
        for year, group in result.groupby("Year"):
            pop = group["Population"].replace(0, np.nan)
            ratio = group[deaths_col] / pop
            # Correction: éviter les warnings numpy sur les slices vides
            if ratio.notna().any():
                median = ratio.median(skipna=True)
                p90 = ratio.quantile(0.9)
            else:
                median = 0
                p90 = 0
            flag_median.extend((ratio > median).astype(int))
            flag_p90.extend((ratio > p90).astype(int))
            flag_abs.extend((group[deaths_col] > 1000).astype(int))
            flag_any.extend((group[deaths_col] > 0).astype(int))
        result[f"{base}_sig_median"] = flag_median
        result[f"{base}_sig_p90"] = flag_p90
        result[f"{base}_sig_abs1000"] = flag_abs
        result[f"{base}_sig_anydeaths"] = flag_any

    # --- SIGNIFICANT EVENT FLAGS FOR GEOMET (if intensity columns exist) ---
    for dtype in DISASTER_TYPES:
        base = dtype.lower().replace(" ", "_")
        intensity_col = f"{base}_intensity"
        if intensity_col in result.columns:
            flag_p90 = []
            for year, group in result.groupby("Year"):
                p90 = group[intensity_col].quantile(0.9)
                flag_p90.extend((group[intensity_col] > p90).astype(int))
            result[f"{base}_geomet_sig_p90"] = flag_p90

    # --- EXTREME EVENT INDICATORS ---
    # For each disaster type, create extreme_*_emdat and extreme_*_geomet columns
    for dtype in DISASTER_TYPES:
        base = dtype.lower().replace(" ", "_")
        # EM-DAT: extreme if sig_p90
        sig_p90_col = f"{base}_sig_p90"
        extreme_emdat_col = f"extreme_{base}_emdat"
        if sig_p90_col in result.columns:
            result[extreme_emdat_col] = result[sig_p90_col]
        # GeoMet: extreme if geomet_sig_p90
        geomet_sig_p90_col = f"{base}_geomet_sig_p90"
        extreme_geomet_col = f"extreme_{base}_geomet"
        if geomet_sig_p90_col in result.columns:
            result[extreme_geomet_col] = result[geomet_sig_p90_col]

    sig_cols = [col for col in result.columns if any(
        col.endswith(suffix) for suffix in ["_sig_median", "_sig_p90", "_sig_abs1000", "_sig_anydeaths", "_geomet_sig_p90"]
    )]
    sig_summary = {col: int(result[col].sum()) for col in sig_cols}
    if sig_summary:
        logger.info("\nRésumé des événements significatifs (nombre d'années/pays avec événement):")
        for col, count in sig_summary.items():
            logger.info(f"  {col}: {count}")
    else:
        logger.info("Aucun indicateur d'événement significatif trouvé dans le panel.")

    # --- EXTREME EVENT INDICATORS (for R tables 5/6) ---
    # Pour chaque type, 1 si sig_p90==1, 0 sinon
    for dtype in DISASTER_TYPES:
        base = dtype.lower().replace(" ", "_")
        # EM-DAT extrêmes (top 10% morts/pop)
        sig_col = f"{base}_sig_p90"
        extreme_col = f"extreme_{base}_emdat"
        if sig_col in result.columns:
            result[extreme_col] = (result[sig_col] == 1).astype(int)
        # GeoMet extrêmes (top 10% intensité)
        geomet_sig_col = f"{base}_geomet_sig_p90"
        extreme_geomet_col = f"extreme_{base}_geomet"
        if geomet_sig_col in result.columns:
            result[extreme_geomet_col] = (result[geomet_sig_col] == 1).astype(int)

    # Harmonisation stricte des colonnes World Bank (pas de if, on force les bons noms partout)
    # Population, is_poor_country, is_small_country, Income group
    required_wb_cols = ['is_poor_country', 'is_small_country', 'Income group', 'Population']
    missing_wb_cols = [col for col in required_wb_cols if col not in result.columns]
    if missing_wb_cols:
        logger.error(f"Colonnes World Bank manquantes dans le dataset final: {missing_wb_cols}")
        raise ValueError(f"Colonnes World Bank manquantes dans le dataset final: {missing_wb_cols}. Vérifiez la préparation et le merge des données World Bank.")
    # Vérification stricte : aucune valeur manquante autorisée dans les colonnes World Bank annuelles
    for col in ['is_poor_country', 'is_small_country', 'Population']:
        if result[col].isnull().any():
            n_missing = result[col].isnull().sum()
            logger.error(f"{n_missing} valeurs manquantes dans la colonne {col} après merge World Bank. Arrêt pipeline.")
            missing_rows = result[result[col].isnull()][["ISO", "Year", "Country"]]
            logger.error(f"Exemples de (ISO, Year) sans info World Bank pour {col}:\n{missing_rows.head(10).to_string(index=False)}")
            logger.error(f"Liste complète des ISO manquants: {sorted(missing_rows['ISO'].unique())}")
            raise ValueError(f"{n_missing} valeurs manquantes dans la colonne {col} après merge World Bank. Vérifiez la couverture des données World Bank et la correspondance des ISO/Year.")
    # Pour Income group : warning unique par ISO manquant, mais pas d'arrêt pipeline
    if result['Income group'].isnull().any():
        missing_iso = result[result['Income group'].isnull()]['ISO'].unique()
        logger.warning(f"{len(missing_iso)} ISO sans Income group dans World Bank : {sorted(missing_iso)} (valeurs NA imputées)")
        result['Income group'] = result['Income group'].fillna('NA')
    # Typage strict (sans fillna)
    result['is_poor_country'] = result['is_poor_country'].astype(int)
    result['is_small_country'] = result['is_small_country'].astype(int)
    result['Population'] = result['Population'].astype(float)
    result['Income group'] = result['Income group'].astype(str)
    logger.trace(f"Colonnes World Bank dans le dataset final: {[c for c in result.columns if 'poor' in c or 'small' in c or 'Income group' in c or 'Population' in c]}")

    # --- FILTRAGE STRICT SUR LA PÉRIODE DEMANDÉE ET SUPPRESSION DES PAYS MANQUANTS ---
    n_before = len(result)
    result = result[(result["Year"] >= year_start) & (result["Year"] <= year_end)]
    n_after_period = len(result)
    if n_after_period < n_before:
        logger.warning(f"{n_before - n_after_period} lignes hors période [{year_start}-{year_end}] supprimées du panel final.")
    n_before_country = len(result)
    result = result[result["Country"].notna()]
    n_after_country = len(result)
    if n_after_country < n_before_country:
        logger.warning(f"{n_before_country - n_after_country} lignes supprimées car Country=NaN après filtrage période.")

    # --- FILTRAGE FINAL : années et pays valides ---
    before = len(result)
    result = result[(result['Year'] >= year_start) & (result['Year'] <= year_end)]
    logger.info(f"Filtrage années [{year_start}, {year_end}] : {before - len(result)} lignes supprimées")
    before2 = len(result)
    result = result[result['Country'].notna()]
    logger.info(f"Filtrage Country non-NaN : {before2 - len(result)} lignes supprimées")

    # Summary and preview
    n_obs = len(result)
    an_min = result['Year'].min() if 'Year' in result.columns else 'N/A'
    an_max = result['Year'].max() if 'Year' in result.columns else 'N/A'
    n_iso = result['ISO'].nunique() if 'ISO' in result.columns else 'N/A'

    # --- LOG: Poor and Small Country Counts for Reference Year ---
    ref_year = config.get('POOR_COUNTRY_YEAR', 2016)
    ref_panel = result[result['Year'] == ref_year]
    n_poor = ref_panel['is_poor_country'].sum() if 'is_poor_country' in ref_panel.columns else 'N/A'
    n_small = ref_panel['is_small_country'].sum() if 'is_small_country' in ref_panel.columns else 'N/A'

    logger.info(
        f"\n📊 DISASTER SUMMARY {year_start}-{year_end}\n"
        f"  • Observations : {n_obs:,}\n"
        f"  • Years        : {an_min}–{an_max}\n"
        f"  • Countries    : {n_iso}\n"
        f"  • Reference year: {ref_year} | Poor countries: {n_poor} | Small countries: {n_small}\n"
    )
    # Affichage concis d'un échantillon
    preview_cols = [col for col in result.columns if any(s in col for s in ["Year", "Country", "ISO", "deaths", "events"])]
    logger.debug(f"\nAperçu (quelques lignes):\n{result[preview_cols].head(5).to_string(index=False)}")

    # --- DISASTER INDEX (GeoMet composite, normalisé) ---
    # 1. Chercher toutes les colonnes *_intensity (GeoMet)
    intensity_cols = [col for col in result.columns if col.endswith('_intensity')]
    if intensity_cols:
        # 2. Normaliser chaque colonne (écart-type sur tout le panel)
        norm_intensities = []
        for col in intensity_cols:
            std = result[col].std(skipna=True)
            if std > 0:
                norm = result[col] / std
            else:
                norm = result[col]
            norm_intensities.append(norm)
        # 3. Somme pondérée (somme des intensités normalisées)
        result['disaster_index'] = sum(norm_intensities)
        logger.info(f"Colonne disaster_index créée à partir de {intensity_cols} (somme des intensités normalisées)")
    else:
        logger.warning("Aucune colonne *_intensity trouvée pour calculer disaster_index (GeoMet)")

    # Save to cache
    result.to_pickle(cache_file)
    logger.info(f"Cache saved: {cache_file}")

    return result

# Main CLI entry
if __name__ == "__main__":
    print("\n==================================\n   🌪️ PREPROCESS EM-DAT & GEOMET (2/4)   \n==================================\n")
    clear_cache = get_pipeline_options()
    # Utilise les périodes du config.json
    for (start, end) in EXPORT_PERIODS:
        period_str = f"{start}_{end}"
        logger.info(f"\n——— 🌪️ Period {period_str.replace('_', ' ')} ———")
        df = create_disaster_dataset(start, end, clear_cache=clear_cache)
    logger.success(f"🌪️ Disasters data pipeline completed successfully! cf {CACHE_DIR}")
