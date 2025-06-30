#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PIPELINE UTILITIES - FONCTIONS INDÉPENDANTES
============================================

Ce module contient les fonctions utilitaires nécessaires au pipeline,
extraites et adaptées de generate_data.py pour assurer l'indépendance totale.
"""

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


def get_exports_dataframe(
    input_path: str = "data/exports/",
    year_start: int = 1979,
    year_end: int = 2024,
    max_records: int = 5000,
    replace: bool = False,
    fetch_missing: bool = False,
) -> pd.DataFrame:
    """
    Génère un DataFrame d'exports filtré pour la période spécifiée.

    Version adaptée et indépendante extraite de generate_data.py

    Args:
        input_path: Chemin vers les fichiers d'exports
        year_start: Année de début
        year_end: Année de fin
        max_records: Nombre max d'enregistrements par requête
        replace: Remplacer les fichiers existants
        fetch_missing: Télécharger les années manquantes

    Returns:
        DataFrame avec les données d'exports filtrées
    """

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

    # Rechercher tous les fichiers d'exports
    all_export_files = sorted(glob.glob(f"{input_path}*exports*.csv"))

    # Mapping direct fichier -> années
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

    # Logs de diagnostic
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
        logger.info(f"Années déjà présentes localement: {sorted([int(y) for y in years_covered])}")

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
            logger.warning("🔄 Téléchargement non supporté dans cette version pipeline")
            return pd.DataFrame()

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

    # Résumé final avec vraies données
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
            logger.warning(f"❌ Year {year}: No data available")
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
    import re
    start_time = time.time()
    all_dfs_exports = []
    skipped_years = []
    quota_exceeded = False

    os.makedirs(output_path, exist_ok=True)

    # --- NOUVEAU : détection des années déjà couvertes localement (multi-années inclus) ---
    export_files = sorted(glob.glob(os.path.join(output_path, '*exports*.csv')))
    years_covered = set()
    def extract_years_from_filename(filename):
        range_match = re.search(r"(\d{4})-(\d{4})_exports", filename)
        if range_match:
            start_year = int(range_match.group(1))
            end_year = int(range_match.group(2))
            return list(range(start_year, end_year + 1))
        single_year_match = re.search(r"(\d{4})_exports", filename)
        if single_year_match:
            return [int(single_year_match.group(1))]
        return []
    for file in export_files:
        years_covered.update(extract_years_from_filename(os.path.basename(file)))
    target_years = set(range(year_start, year_end + 1))
    missing_years = sorted(target_years - years_covered)
    logger.debug(f"Années déjà couvertes: {sorted(years_covered)}")
    if missing_years:
        logger.info(f"📅 Années à télécharger: {missing_years}")
    else:
        logger.debug("✅ Aucune donnée manquante")
        return pd.DataFrame()  # Rien à faire

    # --- Boucle de téléchargement uniquement sur les années manquantes ---
    for year in missing_years:
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
            actual_years = (
                sorted([int(y) for y in df_partial["refYear"].unique()])
                if "refYear" in df_partial.columns
                else []
            )
            logger.info(f"📊 Années téléchargées: {actual_years}")
            logger.info(f"🚫 Années skippées: {[int(y) for y in skipped_years]}")
            logger.info(f"⏱️ Temps: {elapsed_min:.2f} minutes")
            return df_partial
        return pd.DataFrame()
    if all_dfs_exports:
        df_exports = pd.concat(all_dfs_exports, ignore_index=True)
        actual_years = (
            sorted([int(y) for y in df_exports["refYear"].unique()])
            if "refYear" in df_exports.columns
            else []
        )
        logger.info(f"📊 Années téléchargées: {actual_years}")
        logger.info(f"🚫 Années skippées: {[int(y) for y in skipped_years]}")
        logger.info(f"⏱️ Temps: {elapsed_min:.2f} minutes")
        return df_exports
    else:
        logger.warning(f"❌ Aucune donnée récupérée (⏱️ {elapsed_min:.2f} min)")
        return pd.DataFrame()


def clean_iso_codes(df, iso_col="ISO", exclude_iso_codes=None):
    """
    Nettoie les codes ISO dans un DataFrame et exclut éventuellement certains codes.

    Args:
        df: DataFrame contenant une colonne de codes ISO
        iso_col: Nom de la colonne contenant les codes ISO
        exclude_iso_codes: Liste ou ensemble de codes ISO à exclure (optionnel)

    Returns:
        DataFrame avec codes ISO nettoyés et exclus
    """
    df[iso_col] = df[iso_col].astype(str).str.strip().str.upper()
    df = df[df[iso_col].notna() & (df[iso_col] != "NAN")]
    df = df[df[iso_col].str.len() == 3]
    if exclude_iso_codes is not None:
        exclude_set = set([code.strip().upper() for code in exclude_iso_codes])
        df = df[~df[iso_col].isin(exclude_set)]
    return df


def validate_dataframe_structure(
    df: pd.DataFrame, required_cols: list, df_name: str = "DataFrame"
) -> bool:
    """
    Valide la structure d'un DataFrame.

    Args:
        df: DataFrame à valider
        required_cols: Liste des colonnes requises
        df_name: Nom du DataFrame pour les logs

    Returns:
        True si structure valide, False sinon
    """
    if df.empty:
        logger.warning(f"❌ {df_name} est vide")
        return False

    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        logger.error(f"❌ {df_name} - Colonnes manquantes: {missing_cols}")
        return False

    logger.debug(f"✅ {df_name} - Structure valide ({len(df)} lignes)")
    return True


def get_project_root() -> Path:
    """
    Retourne le répertoire racine du projet.

    Returns:
        Path vers le répertoire racine du projet
    """
    current_file = Path(__file__)

    # Si on est dans pipeline/, remonter d'un niveau
    if current_file.parent.name == "pipeline":
        return current_file.parent.parent
    else:
        return current_file.parent


def ensure_directory_exists(path: Path) -> None:
    """
    S'assure qu'un répertoire existe, le crée sinon.

    Args:
        path: Chemin vers le répertoire
    """
    path.mkdir(parents=True, exist_ok=True)


def log_dataframe_summary(df: pd.DataFrame, name: str) -> None:
    """
    Affiche un résumé d'un DataFrame dans les logs.

    Args:
        df: DataFrame à résumer
        name: Nom du DataFrame pour les logs
    """
    if df.empty:
        logger.warning(f"📊 {name}: DataFrame vide")
        return

    logger.info(f"📊 {name}: {len(df):,} lignes × {len(df.columns)} colonnes")

    # Afficher les années si disponibles
    year_cols = [
        col for col in df.columns if col.lower() in ["year", "refyear", "start year"]
    ]
    if year_cols:
        year_col = year_cols[0]
        years = sorted(df[year_col].dropna().unique())
        if years:
            year_range = (
                f"{min(years)}-{max(years)}" if len(years) > 1 else str(years[0])
            )
            logger.info(f"   📅 Années: {year_range}")

    # Afficher les pays si disponibles
    country_cols = [
        col for col in df.columns if col.lower() in ["iso", "reporteriso", "country"]
    ]
    if country_cols:
        country_col = country_cols[0]
        n_countries = df[country_col].nunique()
        logger.info(f"   🌍 Pays: {n_countries}")
