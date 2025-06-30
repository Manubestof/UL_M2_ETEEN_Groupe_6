#!/usr/bin/env python
"""
Pipeline principal révisé - Analyse complète de l'impact des catastrophes naturelles sur les exportations.

Ce script orchestre l'ensemble du pipeline d'analyse en respectant strictement la méthodologie
de l'article de référence et en utilisant uniquement les paramètres centralisés dans config.json.
Toutes les étapes sont indépendantes, configurables, et documentées dans le README.
"""

import sys
import os
import subprocess
import time
import argparse
from pathlib import Path
from loguru import logger
import json

# Chargement de la configuration centralisée
CONFIG_PATH = Path(__file__).parent / "config.json"
with open(CONFIG_PATH, "r") as f:
    config = json.load(f)

USE_CACHE = config.get("USE_CACHE", True)
CLEAR_CACHE = config.get("CLEAR_CACHE", False)
DATA_DIR = Path(config.get("DATA_DIR", "data"))
CACHE_DIR = Path(config.get("CACHE_DIR", "cache"))
RESULTS_DIR = Path(config.get("RESULTS_DIR", "results"))
TABLES_DIR = Path(config.get("TABLES_DIR", "tables"))
EXCLUDED_ISO_CODES = config.get("EXCLUDED_ISO_CODES", [])
PERIODS = config.get("EXPORT_PERIODS", [])
DISASTER_TYPES = config.get("DISASTER_TYPES", [])

print("="*100)
print("   🚦 PIPELINE CONFIGURATION   ")
print("="*100)
print(f"🗂️  DATA_DIR      : {DATA_DIR}")
print(f"🗃️  CACHE_DIR     : {CACHE_DIR}")
print(f"📊 RESULTS_DIR   : {RESULTS_DIR}")
print(f"📑 TABLES_DIR    : {TABLES_DIR}")
print(f"🗝️  USE_CACHE     : {USE_CACHE}")
print(f"🧹 CLEAR_CACHE   : {CLEAR_CACHE}")
print(f"📆 EXPORT_PERIODS: {PERIODS}")
print(f"🌪️  DISASTER_TYPES: {DISASTER_TYPES}")
print("🔒 EXCLUDED_ISO_CODES:")
for i in range(0, len(EXCLUDED_ISO_CODES), 10):
    print("\t" + ", ".join(EXCLUDED_ISO_CODES[i:i+10]))
print("="*100 + "\n")

PIPELINE_STEPS = [
    ("01_collect_exports_data.py", "Collecte et nettoyage des exports (UN Comtrade)"),
    ("02_collect_disasters_data.py", "Collecte et préparation des catastrophes (EM-DAT, GeoMet)"),
    ("03_validate_datasets.py", "Fusion, validation, création des variables finales, datasets finaux"),
    ("04_econometric_analysis.R", "Analyse économétrique et génération des tables (LaTeX, CSV)")
]

PIPELINE_DIR = Path(__file__).parent

# Logging
logger.remove()
logger.add(sys.stderr, level=config.get("LOG_LEVEL", "INFO"))


def run_step(idx, step_file, description, rscript=False):
    logger.info(f"[STEP {idx+1}] {description}\n")
    script_path = PIPELINE_DIR / step_file
    if not script_path.exists():
        logger.error(f"Fichier manquant : {script_path}")
        return False
    cmd = ["Rscript", str(script_path)] if rscript else [sys.executable, str(script_path)]
    if step_file.endswith(".R"):
        cmd = ["Rscript", str(script_path)]
    try:
        result = subprocess.run(cmd, check=True)
        logger.success(f"✅ Étape terminée : {step_file}")
        print("="*100)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Erreur lors de l'exécution de {step_file} : {e}")
        print("="*100)
        return False


def main():
    parser = argparse.ArgumentParser(description="Orchestrateur du pipeline complet")
    parser.add_argument("--step", type=int, help="Exécuter une étape précise (1-4)")
    parser.add_argument("--clear_cache", action="store_true", help="Forcer le rafraîchissement du cache (si supporté par l'étape)")
    parser.add_argument("--fetch_missing", action="store_true", help="Forcer la récupération des années manquantes (si supporté)")
    args = parser.parse_args()

    # Exécution d'une étape précise
    if args.step:
        idx = args.step - 1
        if idx < 0 or idx >= len(PIPELINE_STEPS):
            logger.error("Numéro d'étape invalide. Choisir entre 1 et 4.")
            sys.exit(1)
        step_file, description = PIPELINE_STEPS[idx]
        run_step(idx, step_file, description, rscript=step_file.endswith(".R"))
        return

    # Exécution séquentielle de toutes les étapes
    for idx, (step_file, description) in enumerate(PIPELINE_STEPS):
        ok = run_step(idx, step_file, description, rscript=step_file.endswith(".R"))
        if not ok:
            logger.error(f"Arrêt du pipeline à l'étape {idx+1}.")
            sys.exit(1)
    logger.success("\n🎉 Pipeline complet exécuté avec succès !")

if __name__ == "__main__":
    main()
