from loguru import logger
import sys
import os
import pandas as pd
import numpy as np
import io
import contextlib
import time
import glob
import comtradeapicall
import pickle
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get API key from environment variable
API_KEY = os.getenv("COMTRADE_API_KEY")
if not API_KEY:
    logger.warning(
        "⚠️ COMTRADE_API_KEY not found in environment variables. API calls may fail."
    )
    API_KEY = ""

# Set up logging
logger.remove()
logger.add(sys.stderr, level="DEBUG")


def _check_year_has_data(
    year: int, breakdown_mode: str = "plus", max_records: int = 100
) -> bool:
    """Vérifie rapidement si une année a des données disponibles."""
    try:
        f = io.StringIO()
        with contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
            test_df = comtradeapicall.previewFinalData(
                typeCode="C",
                freqCode="A",
                clCode="HS",
                period=year,
                reporterCode=None,
                cmdCode=None,
                flowCode="X",
                partnerCode="0",
                partner2Code=None,
                customsCode=None,
                motCode=None,
                maxRecords=max_records,
                format_output="JSON",
                aggregateBy=None,
                breakdownMode=breakdown_mode,
                countOnly=None,
                includeDesc=True,
            )

        api_output = f.getvalue()

        if "403" in api_output or "quota" in api_output.lower():
            logger.error(
                f"🚫 QUOTA API ÉPUISÉ détecté lors de la vérification de l'année {year}"
            )
            raise Exception("API_QUOTA_EXCEEDED")

        if test_df is not None and len(test_df) > 0:
            logger.debug(f"✅ Year {year}: Data available ({len(test_df)} records)")
            return True
        else:
            logger.info(f"❌ Year {year}: No data available")
            return False

    except Exception as e:
        if "API_QUOTA_EXCEEDED" in str(e):
            raise e
        logger.warning(f"⚠️ Year {year}: Error during data check - {e}")
        return False


def fetch_comtrade_exports(
    output_path: str,
    breakdown_mode: str = "plus",
    replace: bool = False,
    max_records: int = 5000,
    year_start: int = 1979,
    year_end: int = 2024,
):
    """Télécharge les données Comtrade pour une période donnée."""
    start_time = time.time()
    all_dfs_exports = []
    skipped_years = []
    quota_exceeded = False

    os.makedirs(output_path, exist_ok=True)

    for year in range(year_start, year_end + 1):
        if quota_exceeded:
            logger.error(
                f"❌ Arrêt à cause du quota épuisé. Données partielles jusqu'à l'année {year-1}"
            )
            break

        output_file = os.path.join(output_path, f"{year}_exports_{breakdown_mode}.csv")

        if not replace and os.path.exists(output_file):
            try:
                existing_df = pd.read_csv(output_file)
                if len(existing_df) > 0:
                    all_dfs_exports.append(existing_df)
                    logger.trace(
                        f"📁 Loaded existing data for {year}: {len(existing_df)} records"
                    )
                else:
                    skipped_years.append(year)
            except:
                logger.warning(f"⚠️ Could not load existing file for {year}")
                skipped_years.append(year)
            continue

        # Vérification avec détection quota
        try:
            has_data = _check_year_has_data(year, breakdown_mode, max_records=100)
            if not has_data:
                skipped_years.append(year)
                continue
        except Exception as e:
            if "API_QUOTA_EXCEEDED" in str(e):
                quota_exceeded = True
                break
            else:
                skipped_years.append(year)
                continue

        # Téléchargement des données
        logger.info(f"🔄 Traitement année {year}...")
        df_year_cmd = []

        for cmd_code in range(1, 100):
            if quota_exceeded:
                break

            try:
                f = io.StringIO()
                with contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
                    df = comtradeapicall.previewFinalData(
                        typeCode="C",
                        freqCode="A",
                        clCode="HS",
                        period=year,
                        reporterCode=None,
                        cmdCode=f"{cmd_code:02}",
                        flowCode="X",
                        partnerCode="0",
                        partner2Code=None,
                        customsCode=None,
                        motCode=None,
                        maxRecords=max_records,
                        format_output="JSON",
                        aggregateBy=None,
                        breakdownMode=breakdown_mode,
                        countOnly=None,
                        includeDesc=True,
                    )

                api_output = f.getvalue()

                if "403" in api_output or "quota" in api_output.lower():
                    logger.error(
                        f"🚫 QUOTA API ÉPUISÉ à l'année {year}, produit {cmd_code}"
                    )
                    quota_exceeded = True
                    break

                if df is None or len(df) == 0:
                    continue

                if len(df) >= max_records:
                    logger.warning(
                        f"Year {year}, Code {cmd_code:02}: Max records exceeded ({max_records})"
                    )
                    continue

                df_year_cmd.append(df)

            except Exception as e:
                logger.warning(f"⚠️ Erreur année {year}, produit {cmd_code}: {e}")
                continue

        if quota_exceeded:
            break

        # Sauvegarder les données de l'année
        if df_year_cmd:
            df_year = pd.concat(df_year_cmd, ignore_index=True)
            df_year.to_csv(output_file, index=False)
            logger.info(f"✅ Année {year}: {len(df_year)} enregistrements sauvegardés")
            all_dfs_exports.append(df_year)
        else:
            skipped_years.append(year)

    # Résumé final
    elapsed_min = (time.time() - start_time) / 60

    if quota_exceeded:
        logger.warning("⚠️ DONNÉES INCOMPLÈTES - Quota API épuisé")
        if all_dfs_exports:
            df_partial = pd.concat(all_dfs_exports, ignore_index=True)
            logger.info(f"📊 Données partielles: {len(df_partial)} lignes")
            return df_partial
        return pd.DataFrame()

    if all_dfs_exports:
        df_exports = pd.concat(all_dfs_exports, ignore_index=True)
        # ✅ LOGS CORRECTS : Afficher les vraies années disponibles
        actual_years = (
            sorted(df_exports["refYear"].unique())
            if "refYear" in df_exports.columns
            else []
        )
        logger.info(f"📊 Années téléchargées: {actual_years}")
        logger.info(f"🚫 Années skippées: {skipped_years}")
        logger.info(f"⏱️ Temps: {elapsed_min:.2f} minutes")
        return df_exports
    else:
        logger.warning(f"❌ Aucune donnée récupérée (⏱️ {elapsed_min:.2f} min)")
        return pd.DataFrame()


def get_exports_dataframe(
    input_path: str = "data/exports/",
    year_start: int = 1979,
    year_end: int = 2024,
    max_records: int = 5000,
    replace: bool = False,
    fetch_missing: bool = False,
) -> pd.DataFrame:
    """Génère un DataFrame d'exports filtré pour la période spécifiée."""
    import re

    all_export_files = sorted(glob.glob(f"{input_path}*exports*.csv"))
    logger.info(input_path)

    def extract_years_from_filename(filename):
        """Extrait les années d'un nom de fichier."""
        # Pattern 1: Plage d'années - YYYY-YYYY_exports (avec suffixes possibles)
        range_match = re.search(r"(\d{4})-(\d{4})_exports", filename)
        if range_match:
            start_year = int(range_match.group(1))
            end_year = int(range_match.group(2))
            return list(range(start_year, end_year + 1))

        # Pattern 2: Année simple - YYYY_exports (avec suffixes possibles)
        single_year_match = re.search(r"(\d{4})_exports", filename)
        if single_year_match:
            return [int(single_year_match.group(1))]

        return []

    # ✅ SIMPLIFICATION : Mapping direct fichier -> années
    file_coverage = {}
    for file in all_export_files:
        filename = os.path.basename(file)
        years = extract_years_from_filename(filename)
        if years:
            file_coverage[file] = years
            year_range = (
                f"{min(years)}-{max(years)}" if len(years) > 1 else str(years[0])
            )
            logger.trace(f"📁 {filename} → {year_range}")

    # Sélectionner les fichiers qui couvrent la période demandée
    target_years = set(range(year_start, year_end + 1))
    files_to_load = []
    years_covered = set()

    for file_path, file_years in file_coverage.items():
        overlap = set(file_years).intersection(target_years)
        if overlap:
            files_to_load.append(file_path)
            years_covered.update(overlap)

            year_range = (
                f"{min(overlap)}-{max(overlap)}"
                if len(overlap) > 1
                else str(list(overlap)[0])
            )
            logger.trace(f"✅ {os.path.basename(file_path)} → couvre {year_range}")

    missing_years = sorted(target_years - years_covered)

    # ✅ LOGS CORRECTS
    logger.debug(
        f"📊 Période demandée: {year_start}-{year_end} ({len(target_years)} années)"
    )
    logger.debug(f"📁 Fichiers sélectionnés: {len(files_to_load)}")

    if years_covered:
        actual_range = (
            f"{min(years_covered)}-{max(years_covered)}"
            if len(years_covered) > 1
            else str(list(years_covered)[0])
        )
        logger.debug(
            f"✅ Années réellement couvertes: {actual_range} ({len(years_covered)} années)"
        )

    if missing_years:
        logger.warning(f"❌ Années manquantes: {missing_years}")

    # Chargement des fichiers
    if not files_to_load:
        if not fetch_missing:
            logger.warning(
                "❌ Aucun fichier trouvé. Utilisez fetch_missing=True pour télécharger."
            )
            return pd.DataFrame()
        else:
            logger.info("🔄 Téléchargement complet...")
            return fetch_comtrade_exports(
                input_path, "plus", replace, max_records, year_start, year_end
            )

    # Charger les fichiers existants
    exports_all = []
    for file in files_to_load:
        try:
            df = pd.read_csv(
                file, encoding="latin1", sep=None, engine="python", index_col=False
            )
            exports_all.append(df)
            logger.trace(f"✅ Chargé: {os.path.basename(file)}")
        except Exception as e:
            logger.error(f"❌ Erreur lors du chargement de {file}: {e}")

    if not exports_all:
        logger.warning("❌ Aucun fichier n'a pu être chargé")
        return pd.DataFrame()

    exports = pd.concat(exports_all, ignore_index=True)
    logger.debug(f"📊 Chargement réussi : {len(exports)} lignes")

    # Télécharger les années manquantes si demandé
    if missing_years and fetch_missing:
        logger.info(f"🔄 Téléchargement des années manquantes: {missing_years}...")
        new_exports = fetch_comtrade_exports(
            input_path,
            "plus",
            False,
            max_records,
            min(missing_years),
            max(missing_years),
        )
        if not new_exports.empty:
            exports = pd.concat(
                [exports, new_exports], ignore_index=True
            ).drop_duplicates()
            logger.success(f"✅ Nouvelles données ajoutées : {len(new_exports)} lignes")

    # Nettoyage et préparation
    if exports.empty:
        logger.error("❌ Aucune donnée d'export disponible")
        return pd.DataFrame()

    # Créer l'indicateur agricole
    exports["is_agri"] = exports["cmdCode"].isin(range(1, 25))

    # Filtrer pour la période demandée et nettoyer
    exports_filtered = exports[
        (exports["refYear"] >= year_start) & (exports["refYear"] <= year_end)
    ][
        [
            "reporterISO",
            "reporterDesc",
            "refYear",
            "cmdCode",
            "cmdDesc",
            "is_agri",
            "fobvalue",
        ]
    ].copy()

    exports_filtered = exports_filtered.dropna(subset=["fobvalue"]).query(
        "fobvalue > 0"
    )

    # ✅ RÉSUMÉ CORRECT avec vraies données
    if not exports_filtered.empty:
        actual_years = sorted(exports_filtered["refYear"].unique())
        actual_range = (
            f"{min(actual_years)}-{max(actual_years)}"
            if len(actual_years) > 1
            else str(actual_years[0])
        )
        n_agri = exports_filtered["is_agri"].sum()
        pct_agri = exports_filtered["is_agri"].mean() * 100

        logger.info(f"📊 EXPORTS DATA ({actual_range}):")
        logger.info(
            f"   {len(exports_filtered):,} obs, {exports_filtered['reporterISO'].nunique()} countries, {exports_filtered['cmdCode'].nunique()} products"
        )
        logger.info(f"   Agricultural: {n_agri:,} ({pct_agri:.1f}%)")
    else:
        logger.warning(f"❌ Aucune donnée disponible pour {year_start}-{year_end}")

    return exports_filtered


# ✅ SIMPLIFICATION : Autres fonctions nettoyées (enlever lignes dupliquées et logs trompeurs)
def load_emdat_data(year_start=1979, year_end=2024, disasters_list=None):
    """Charge et filtre les données EM-DAT."""
    if disasters_list is None:
        disasters_list = ["Earthquake", "Storm", "Flood", "Extreme temperature"]

    all_emdat = []

    # EM-DAT 1979-2000
    if year_start < 2000:
        try:
            emdat_1979 = pd.read_excel(
                "data/emdat/EM-DAT 1979-2000.xlsx", sheet_name="EM-DAT Data"
            )
            emdat_1979 = emdat_1979[emdat_1979["Disaster Type"].isin(disasters_list)]
            emdat_1979 = emdat_1979.rename(columns={"Start Year": "Year"})
            emdat_1979 = emdat_1979[
                (emdat_1979["Year"] >= year_start)
                & (emdat_1979["Year"] <= min(year_end, 1999))
            ]

            if not emdat_1979.empty:
                all_emdat.append(emdat_1979)
                actual_years = sorted(emdat_1979["Year"].unique())
                logger.debug(
                    f"EM-DAT 1979-2000: {len(emdat_1979):,} lignes, années {min(actual_years)}-{max(actual_years)}"
                )
        except FileNotFoundError:
            logger.warning("Fichier EM-DAT 1979-2000 introuvable")

    # EM-DAT 2000+
    if year_end >= 2000:
        try:
            emdat_2000 = pd.read_excel(
                "data/emdat/EM-DAT countries 2000+.xlsx", skiprows=[1]
            )
            emdat_2000 = emdat_2000[emdat_2000["Disaster Type"].isin(disasters_list)]

            if "year" in emdat_2000.columns:
                emdat_2000 = emdat_2000.rename(columns={"year": "Year"})
            if "iso" in emdat_2000.columns:
                emdat_2000 = emdat_2000.rename(columns={"iso": "ISO"})

            emdat_2000 = emdat_2000[
                (emdat_2000["Year"] >= max(year_start, 2000))
                & (emdat_2000["Year"] <= year_end)
            ]

            if not emdat_2000.empty:
                all_emdat.append(emdat_2000)
                actual_years = sorted(emdat_2000["Year"].unique())
                logger.debug(
                    f"EM-DAT 2000+: {len(emdat_2000):,} lignes, années {min(actual_years)}-{max(actual_years)}"
                )
        except FileNotFoundError:
            logger.warning("Fichier EM-DAT 2000+ introuvable")

    if all_emdat:
        combined_emdat = pd.concat(all_emdat, ignore_index=True)
        actual_years = sorted(combined_emdat["Year"].unique())
        actual_range = (
            f"{min(actual_years)}-{max(actual_years)}"
            if len(actual_years) > 1
            else str(actual_years[0])
        )
        logger.debug(
            f"EM-DAT combiné: {len(combined_emdat):,} lignes, années réelles {actual_range}"
        )
        return combined_emdat
    else:
        logger.warning(f"Aucune donnée EM-DAT pour {year_start}-{year_end}")
        return pd.DataFrame()


def load_geomet_data(year_start=1979, year_end=2024):
    """Charge les données GeoMet pour la période spécifiée."""
    try:
        geomet = pd.read_stata("data/geomet/IfoGAME_EMDAT.dta")
        geomet = geomet.rename(columns={"iso": "ISO", "year": "Year"})
        geomet["ISO"] = geomet["ISO"].str.upper()
        geomet = geomet[(geomet["Year"] >= year_start) & (geomet["Year"] <= year_end)]

        if not geomet.empty:
            actual_years = sorted(geomet["Year"].unique())
            year_range = (
                f"{min(actual_years)}-{max(actual_years)}"
                if len(actual_years) > 1
                else str(actual_years[0])
            )
            logger.debug(f"GeoMet: {len(geomet)} lignes, années {year_range}")
        else:
            logger.debug(f"GeoMet: Aucune donnée pour {year_start}-{year_end}")

        return geomet
    except FileNotFoundError:
        logger.warning("Fichier GeoMet introuvable")
        return pd.DataFrame()


def load_worldbank_data(year_start=1979, year_end=2024):
    """Charge les données World Bank (population et classification des revenus)."""
    # Population
    try:
        pop_data = pd.read_excel(
            "data/world_bank/total_population.xlsx", sheet_name="Estimates", header=16
        )
        pop = pop_data[pop_data["Type"] == "Country/Area"].rename(
            columns={
                "ISO3 Alpha-code": "ISO",
                "Total Population, as of 1 July (thousands)": "Population",
            }
        )
        pop["Population"] = pd.to_numeric(pop["Population"], errors="coerce") * 1000
        pop = pop[(pop["Year"] >= year_start) & (pop["Year"] <= year_end)]

        if not pop.empty:
            actual_years = sorted(pop["Year"].unique())
            year_range = (
                f"{min(actual_years)}-{max(actual_years)}"
                if len(actual_years) > 1
                else str(actual_years[0])
            )
            logger.debug(f"Population: {len(pop):,} lignes, années {year_range}")
        else:
            logger.debug(f"Population: Aucune donnée pour {year_start}-{year_end}")

    except FileNotFoundError:
        logger.warning("Fichier population World Bank introuvable")
        pop = pd.DataFrame()

    # Income classification
    try:
        income_data = pd.read_excel(
            "data/world_bank/country_income_classification.xlsx"
        )
        income = income_data.rename(columns={"Code": "ISO"})
        income["is_poor_country"] = income["Income group"].isin(
            ["Low income", "Lower middle income"]
        )

        n_poor = income["is_poor_country"].sum()
        logger.debug(f"Classification revenus: {len(income):,} pays ({n_poor} pauvres)")
    except FileNotFoundError:
        logger.warning("Fichier classification revenus introuvable")
        income = pd.DataFrame()

    return pop, income


def aggregate_emdat(emdat_df):
    """Agrège les données EM-DAT par pays-année-type de catastrophe."""
    if emdat_df.empty:
        return pd.DataFrame()

    # Colonnes à agréger
    agg_cols = {
        col: "sum"
        for col in [
            "Total Deaths",
            "Total Damage ('000 US$)",
            "Total Affected",
            "Total Events",
        ]
        if col in emdat_df.columns
    }

    if not agg_cols:
        logger.warning("Aucune colonne EM-DAT à agréger trouvée")
        return pd.DataFrame()

    # Agrégation et pivot
    emdat_agg = (
        emdat_df.groupby(["ISO", "Year", "Disaster Type"]).agg(agg_cols).reset_index()
    )
    emdat_pivot = emdat_agg.pivot(index=["ISO", "Year"], columns="Disaster Type")

    # Nettoyer les noms de colonnes
    emdat_pivot.columns = [
        "_".join([str(i) for i in col]).replace(" ", "_").lower()
        for col in emdat_pivot.columns
    ]

    logger.debug(f"EM-DAT agrégé: {len(emdat_pivot)} lignes pays-année")
    return emdat_pivot.reset_index()


def aggregate_geomet(geomet_df):
    """Agrège les données GeoMet par pays-année."""
    if geomet_df.empty:
        return pd.DataFrame()

    damage_cols = [
        "damage_gdp_eq",
        "damage_gdp_fld",
        "damage_gdp_str",
        "damage_gdp_temp",
    ]
    available_cols = {col: "max" for col in damage_cols if col in geomet_df.columns}

    if not available_cols:
        logger.warning("Aucune colonne de dommages GeoMet trouvée")
        return pd.DataFrame()

    geomet_agg = geomet_df.groupby(["ISO", "Year"]).agg(available_cols).reset_index()
    logger.debug(f"GeoMet agrégé: {len(geomet_agg)} lignes pays-année")
    return geomet_agg


def create_base_country_data(pop_df, income_df, emdat_pivot, geomet_agg):
    """Crée le dataset de base pays-année avec population, revenus et catastrophes."""
    if pop_df.empty:
        logger.warning("Pas de données de population - dataset vide")
        return pd.DataFrame()

    # Dataset de base
    base_df = pop_df[["ISO", "Year", "Population"]].merge(
        income_df[["ISO", "is_poor_country"]], on="ISO", how="left"
    )

    # Ajouter les catastrophes
    if not emdat_pivot.empty:
        base_df = base_df.merge(emdat_pivot, on=["ISO", "Year"], how="left")
        logger.debug("✅ Données EM-DAT ajoutées")

    if not geomet_agg.empty:
        base_df = base_df.merge(geomet_agg, on=["ISO", "Year"], how="left")
        logger.debug("✅ Données GeoMet ajoutées")

    logger.debug(f"Dataset de base: {len(base_df)} lignes pays-année")
    return base_df


def add_exports_to_country_data(base_df, exports_df, country_names_df):
    """Ajoute les données d'export au dataset pays-année."""
    if exports_df.empty:
        logger.warning("Pas de données d'export")
        country_df = base_df.merge(country_names_df, on="ISO", how="left")
        country_df["total_exports"] = 0
        country_df["exports_agriculture"] = 0
        return country_df, pd.DataFrame()

    # Renommer et agréger
    exports_renamed = exports_df.rename(
        columns={
            "reporterISO": "ISO",
            "refYear": "Year",
            "cmdCode": "Product",
            "fobvalue": "Exports",
        }
    )

    # Total exports par pays-année
    exports_agg = (
        exports_renamed.groupby(["ISO", "Year"])["Exports"]
        .sum()
        .reset_index()
        .rename(columns={"Exports": "total_exports"})
    )

    # Exports agricoles
    exports_agri = (
        exports_renamed[exports_renamed["is_agri"]]
        .groupby(["ISO", "Year"])["Exports"]
        .sum()
        .reset_index()
        .rename(columns={"Exports": "exports_agriculture"})
    )

    # Créer les datasets finaux
    country_df = (
        base_df.merge(country_names_df, on="ISO", how="left")
        .merge(exports_agg, on=["ISO", "Year"], how="left")
        .merge(exports_agri, on=["ISO", "Year"], how="left")
    )

    country_df["total_exports"] = country_df["total_exports"].fillna(0)
    country_df["exports_agriculture"] = country_df["exports_agriculture"].fillna(0)

    # Dataset produit
    product_df = exports_renamed.merge(base_df, on=["ISO", "Year"], how="left").merge(
        country_names_df, on="ISO", how="left"
    )

    # Variables dérivées
    for df in [country_df, product_df]:
        if not df.empty:
            df["is_small_country"] = df["Population"] < 20_000_000
            df["is_poor_country"] = df["is_poor_country"].fillna(False)
            # Remplir les NaN numériques avec 0
            num_cols = df.select_dtypes(include=[np.number]).columns
            df[num_cols] = df[num_cols].fillna(0)

    logger.debug(
        f"Datasets créés: {len(country_df)} pays-année, {len(product_df)} produit-pays-année"
    )
    return country_df, product_df


def add_significant_events(df):
    """Ajoute les variables d'événements significatifs basées sur le ratio décès/population."""
    if df.empty or "Population" not in df.columns:
        logger.warning("Cannot calculate significant events - missing data")
        return df

    disaster_death_cols = [col for col in df.columns if "deaths" in col.lower()]

    if not disaster_death_cols:
        logger.warning("Cannot calculate significant events - no death columns found")
        df["is_significant_event"] = False
        return df

    # Calculs de base
    df["total_deaths"] = df[disaster_death_cols].sum(axis=1)
    df["deaths_pop_ratio"] = df["total_deaths"] / (df["Population"] + 1)

    # Seuil de significativité
    positive_ratios = df[df["total_deaths"] > 0]["deaths_pop_ratio"]

    if len(positive_ratios) > 0:
        median_ratio = positive_ratios.median()
        df["is_significant_event"] = (df["deaths_pop_ratio"] > median_ratio) & (
            df["total_deaths"] > 0
        )

        # Événements significatifs par type de catastrophe
        for col in disaster_death_cols:
            ratio_col = f"{col}_pop_ratio"
            significant_col = (
                f'is_significant_{col.replace("total_deaths_", "").replace("_", "")}'
            )

            df[ratio_col] = df[col] / (df["Population"] + 1)
            df[significant_col] = (df[ratio_col] > median_ratio) & (df[col] > 0)

        n_significant = df["is_significant_event"].sum()
        pct_significant = n_significant / len(df) * 100
        logger.info(
            f"Événements significatifs: {n_significant} ({pct_significant:.1f}%)"
        )
    else:
        df["is_significant_event"] = False
        logger.warning("No events with deaths found")

    return df


def get_country_names_from_exports(exports_df):
    """Extrait les noms de pays des datasets d'export."""
    if exports_df.empty:
        return pd.DataFrame()

    country_names = (
        exports_df[["reporterISO", "reporterDesc"]]
        .drop_duplicates()
        .rename(columns={"reporterISO": "ISO", "reporterDesc": "Country"})
    )

    logger.debug(f"Noms de pays extraits: {len(country_names)} pays")
    return country_names


def get_cache_filename(year_start, year_end, disasters_list):
    """Génère un nom de fichier de cache basé sur les paramètres."""
    disasters_str = "_".join(sorted(disasters_list)) if disasters_list else "all"
    return (
        f"cache/datasets_{year_start}_{year_end}_{disasters_str.replace(' ', '')}.pkl"
    )


def load_cached_datasets(year_start, year_end, disasters_list):
    """Charge les datasets depuis le cache s'ils existent."""
    cache_file = get_cache_filename(year_start, year_end, disasters_list)

    if os.path.exists(cache_file):
        try:
            with open(cache_file, "rb") as f:
                cached_data = pickle.load(f)
            logger.info(f"📁 Datasets chargés depuis le cache: {cache_file}")
            return cached_data["country_df"], cached_data["product_df"]
        except Exception as e:
            logger.warning(f"⚠️ Erreur lors du chargement du cache: {e}")
            return None, None
    return None, None


def save_datasets_to_cache(
    country_df, product_df, year_start, year_end, disasters_list
):
    """Sauvegarde les datasets dans le cache."""
    cache_file = get_cache_filename(year_start, year_end, disasters_list)

    # Créer le dossier cache s'il n'existe pas
    cache_dir = Path(cache_file).parent
    cache_dir.mkdir(exist_ok=True)

    try:
        with open(cache_file, "wb") as f:
            pickle.dump(
                {
                    "country_df": country_df,
                    "product_df": product_df,
                    "created_at": pd.Timestamp.now(),
                    "parameters": {
                        "year_start": year_start,
                        "year_end": year_end,
                        "disasters_list": disasters_list,
                    },
                },
                f,
            )
        logger.info(f"� Datasets sauvegardés dans le cache: {cache_file}")
    except Exception as e:
        logger.warning(f"⚠️ Erreur lors de la sauvegarde du cache: {e}")


def clear_cache():
    """Supprime tous les fichiers de cache."""
    cache_dir = Path("cache")
    if cache_dir.exists():
        for cache_file in cache_dir.glob("datasets_*.pkl"):
            cache_file.unlink()
            logger.info(f"🗑️ Cache supprimé: {cache_file}")
        logger.success("✅ Cache nettoyé !")
    else:
        logger.info("📁 Aucun dossier cache trouvé")


def list_cached_datasets():
    """Liste tous les datasets en cache."""
    cache_dir = Path("cache")
    if not cache_dir.exists():
        print("📁 Aucun dossier cache trouvé")
        return

    cache_files = list(cache_dir.glob("datasets_*.pkl"))
    if not cache_files:
        print("📁 Aucun dataset en cache")
        return

    print("📦 DATASETS EN CACHE:")
    for cache_file in sorted(cache_files):
        try:
            with open(cache_file, "rb") as f:
                cached_data = pickle.load(f)

            created_at = cached_data.get("created_at", "Inconnu")
            params = cached_data.get("parameters", {})

            print(f"  📄 {cache_file.name}")
            print(
                f"     Période: {params.get('year_start', '?')}-{params.get('year_end', '?')}"
            )
            print(f"     Créé: {created_at}")
            print(f"     Pays-année: {len(cached_data['country_df'])} obs")
            print(f"     Produit-pays-année: {len(cached_data['product_df'])} obs")
            print()
        except Exception as e:
            print(f"  ❌ {cache_file.name} (erreur: {e})")


def generate_dataset(
    year_start, year_end, fetch_missing=False, disasters_list=None, use_cache=True
):
    """
    Crée un dataset avec système de cache.

    Args:
        year_start: Année de début
        year_end: Année de fin
        fetch_missing: Télécharger les données manquantes
        disasters_list: Liste des types de catastrophes
        use_cache: Utiliser le cache (défaut: True)

    Returns:
        tuple: (country_df, product_df)
    """
    # Essayer de charger depuis le cache
    if use_cache:
        country_df, product_df = load_cached_datasets(
            year_start, year_end, disasters_list
        )
        if country_df is not None and product_df is not None:
            logger.success(
                f"✅ Datasets {year_start}-{year_end} chargés depuis le cache !"
            )
            return country_df, product_df

    # Si pas de cache, créer les datasets
    logger.info(f"🔄 Création des datasets {year_start}-{year_end}...")

    # 1. Load exports data
    logger.debug("📊 Chargement des données d'export...")
    exports_df = get_exports_dataframe(
        input_path="data/exports/",
        year_start=year_start,
        year_end=year_end,
        max_records=5000,
        replace=False,
        fetch_missing=fetch_missing,
    )

    # 2. Load disaster data
    logger.debug("🌪️ Chargement des données de catastrophes...")
    emdat = load_emdat_data(year_start, year_end, disasters_list)
    geomet = load_geomet_data(year_start, year_end)

    # 3. Load World Bank data
    logger.debug("🌍 Chargement des données World Bank...")
    pop, income = load_worldbank_data(year_start, year_end)

    # 4. Aggregate disaster data
    logger.debug("📈 Agrégation des données de catastrophes...")
    emdat_pivot = aggregate_emdat(emdat)
    geomet_agg = aggregate_geomet(geomet)

    # 5. Create base country dataset
    logger.debug("🏗️ Création du dataset de base pays-année...")
    base_df = create_base_country_data(pop, income, emdat_pivot, geomet_agg)

    # 6. Get country names
    country_names = get_country_names_from_exports(exports_df)

    # 7. Add exports and create final datasets
    logger.debug("💰 Ajout des données d'export...")
    country_df, product_df = add_exports_to_country_data(
        base_df, exports_df, country_names
    )

    # 8. Add significant events
    logger.debug("🔥 Calcul des événements significatifs...")
    country_df = add_significant_events(country_df)

    logger.success(f"✅ Datasets {year_start}-{year_end} créés avec succès !")

    # Sauvegarder dans le cache
    if use_cache and not country_df.empty:
        save_datasets_to_cache(
            country_df, product_df, year_start, year_end, disasters_list
        )
        logger.info(f"💾 Datasets sauvegardés dans le cache")

    return country_df, product_df


def summarize_dataset(df, name):
    """Generate summary statistics for a dataset"""
    if df.empty:
        return f"{name}: 0 observations"

    n_countries = df["ISO"].nunique() if "ISO" in df.columns else 0
    n_years = df["Year"].nunique() if "Year" in df.columns else 0

    # Basic stats
    summary = [f"{name}: {len(df):,} observations"]
    summary.append(f"  → {n_countries} countries, {n_years} years")

    # Country characteristics
    if "is_poor_country" in df.columns and "is_small_country" in df.columns:
        n_poor = (
            df.groupby("ISO")["is_poor_country"].first().sum() if n_countries > 0 else 0
        )
        n_small = (
            df.groupby("ISO")["is_small_country"].first().sum()
            if n_countries > 0
            else 0
        )
        summary.append(f"  → Poor countries: {n_poor}, Small countries: {n_small}")

    # Exports
    if "total_exports" in df.columns:
        n_with_exports = (df["total_exports"] > 0).sum()
        summary.append(f"  → Observations with exports: {n_with_exports:,}")

    # Disasters
    disaster_cols = [
        c
        for c in df.columns
        if any(x in c.lower() for x in ["deaths", "damage", "events"])
    ]
    if disaster_cols:
        n_disasters = (df[disaster_cols] > 0).any(axis=1).sum()
        summary.append(f"  → Observations with disasters: {n_disasters:,}")

    # Significant events
    if "is_significant_event" in df.columns:
        n_significant = df["is_significant_event"].sum()
        summary.append(f"  → Significant events: {n_significant:,}")

    return "\n".join(summary)
