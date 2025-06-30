# Pipeline Révisé - Analyse de l'Impact des Catastrophes Naturelles sur les Exportations

Ce pipeline réimplémente et étend l'analyse économétrique.

## 🎯 Objectifs et Améliorations

- 📊 **Double analyse temporelle** : 1979-2000 ET 2000-2024
- 🌍 **Sources de données multiples** : EM-DAT + GeoMet + World Bank + UN
- 📈 **Méthodologie complète** : Effets fixes, interactions, variables log-diff
- 📄 **Outputs publication** : Tables LaTeX directement utilisables
- 🔄 **Pipeline modulaire** : Étapes indépendantes et reproductibles
- 💾 **Cache intelligent** : Évite téléchargements répétés

## 📁 Structure du Pipeline

```
pipeline/
├── 01_collect_exports_data.py
├── 02_collect_disasters_data.py
├── 03_validate_datasets.py
├── 04_econometric_analysis.R
├── run_pipeline.py
├── config.json
└── utils/
```

## 🔗 Détail des Entrées et Sorties de chaque Étape du Pipeline

| Étape                     | Script                       | Entrées attendues                                                             | Sorties produites                                    | Variables clés attendues                                                                      |
| -------------------------- | ---------------------------- | ------------------------------------------------------------------------------ | ---------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| 1. Collecte exports        | 01_collect_exports_data.py   | Fichiers sources Comtrade, config.json, .env (clé API)                        | cache/exports_combined.pkl, CSVs intermédiaires     | year, iso3, hs2, export_value                                                                  |
| 2. Collecte catastrophes   | 02_collect_disasters_data.py | Fichiers EM-DAT (data/emdat/), config.json                                     | cache/disasters_combined_*.pkl, CSVs intermédiaires | year, iso3, earthquake_events, flood_events, storm_events, temp_events, earthquake_deaths, ... |
| 3. Fusion/validation       | 03_validate_datasets.py      | exports_combined.pkl, disasters_combined_*.pkl, données population/World Bank | datasets/econometric_dataset_*.csv                   | year, iso3, hs2, export_value, earthquake_events, flood_events, storm_events, temp_events, ... |
| 4. Analyse économétrique | 04_econometric_analysis.R    | datasets/econometric_dataset_*.csv                                             | results/tables/*.csv, *.tex, *.rds                   | Toutes les variables ci-dessus + variables d’interaction (is_poor, is_small, etc.)            |

**Remarques importantes** :

- Les variables de catastrophes (`*_events`, `*_deaths`, etc.) doivent contenir des valeurs non nulles pour permettre l’estimation économétrique. Si elles sont toutes nulles, les tables de résultats seront remplies de NA.
- Les scripts vérifient la présence des fichiers d’entrée : si un fichier est absent ou vide, l’étape échoue ou produit des sorties invalides.
- Les outputs de chaque étape servent d’inputs à l’étape suivante : il est crucial de vérifier la cohérence des variables à chaque transition.

**Diagnostic rapide** : Après chaque étape, vérifier que les variables clés (notamment les variables de catastrophes) contiennent des valeurs non nulles dans les outputs intermédiaires (`cache/`, `datasets/`).

> **Diagnostic pipeline** :
>
> - Les variables de catastrophes dans les datasets finaux contiennent bien des valeurs différentes de zéro (voir script `toolkit/diagnostic_catastrophes.py`).
> - Si les tables R sont remplies de NA, cela provient d'un problème de transformation, de filtrage ou de calcul dans la step 04 (R), ou d'un mauvais mapping des variables utilisées dans les modèles.
> - Vérifiez que les variables utilisées dans les formules de régression (ex: `ln_total_occurrence`, `ln_total_deaths`, `disaster_index`) sont bien calculées et non toutes nulles ou NA après transformation.
> - Utilisez le script de diagnostic pour valider la présence de valeurs non nulles à chaque étape.

---

## 🚀 Utilisation

### Prérequis

```bash
# Python packages
pip install -r requirements.txt

# R packages (automatique via le pipeline)
# dplyr, readr, stringr, broom, fixest, modelsummary, xtable
```

### Configuration

1. **Clé API Comtrade** : Créer `.env` avec `COMTRADE_API_KEY=votre_cle` sauf en cas de téléchargement manuel
2. **Données EM-DAT** : S'assurer que les fichiers Excel sont dans `data/emdat/`
3. **R installé** : Vérifier que `Rscript` est disponible

### Exécution

#### Pipeline Complet

```bash
python pipeline/run_pipeline.py
```

#### Étapes Individuelles

```bash
# Étape 1 : Collecte exports
python pipeline/01_collect_exports_data.py

# Étape 2 : Collecte catastrophes  
python pipeline/02_collect_disasters_data.py

# Étape 3 : Création datasets
python pipeline/03_validate_datasets.py

# Étape 4 : Analyse économétrique
Rscript pipeline/04_econometric_analysis.R
```

#### Options

```bash
# Forcer rechargement des données et récupération années manquantes
python pipeline/run_pipeline.py --clear_cache --fetch_missing
```

## 📊 Méthodologie

### Spécification Économétrique

Suivant exactement l'article de référence :

```
ln(EX^k_it) - ln(EX^k_it-1) = c + β₁*disaster_it + β₂*disaster_it×country_i + 
                               β₃*[ln(POP_it) - ln(POP_it-1)] + λᵢᵏ + λₜᵏ + εᵢₜ
```

Où :

- `EX^k_it` : Exportations du produit k, pays i, année t
- `disaster_it` : Variables de catastrophes (occurrence, morts, intensité physique)
- `country_i` : Caractéristiques pays (pauvre/riche, petit/grand)
- `λᵢᵏ, λₜᵏ` : Effets fixes produit×pays et produit×année

## 📚 Critères d'Inclusion et Exclusion

- **Pays inclus (1979-2000)** : 179 pays ISO3 valides
- **Pays avec données complètes** : 50 pays (exports + catastrophes + caractéristiques)
- **Exclus** : DDR, CSK, ANT, SCG, pays sans données World Bank (voir `config.json`)
- **Pays pauvres** : "Low income" ou "Lower middle income" (World Bank 2016)
- **Petits pays** : Population < 20M (médiane échantillon)
- **Catastrophes significatives** : Ratio deaths/population > médiane annuelle/type, types : Earthquakes, Floods, Storms, Extreme temperatures, critères EM-DAT (≥10 morts, ≥100 affectés, état d'urgence, aide internationale)

## 📊 Variables de Catastrophes

- **EM-DAT** : Occurrence (`ln(n_events+1)`), mortalité (`ln(n_morts+1)`), événements significatifs (médiane ratio morts/pop)
- **GeoMet** : Intensité physique (Richter, précipitations, vent, température, index composite)

## 📄 Outputs

- **Cache** : disasters_combined_*.pkl, analysis_country_*.pkl, analysis_product_*.pkl
- **Résultats** : table1_*.csv, table2_poor_*.csv, table3_small_*.csv, dataset_summary.csv, econometric_models_*.rds

## 🔧 Architecture Technique

- Cache intelligent (évite re-téléchargements)
- Logging détaillé
- Validation et diagnostics à chaque étape
- Reproductibilité : seeds fixes, requirements.txt, documentation

## 🚦 Philosophie du pipeline

- Toutes les transformations sont faites en Python (étape 03)
- Le script R (étape 04) ne fait qu’analyser et générer les tables
- Plus de logique redondante ou héritée dans R
- Tables avancées intégrées dans la boucle principale

## 📝 Notes de reproductibilité

- Si NA dans les tables R, vérifier la présence des variables log/groupes dans les CSV produits par l’étape 03.
- Toute la configuration est dans `config.json`.

---

*Pipeline créé le 15 juin 2025 - Révision complète de l'analyse économétrique selon méthodologie article de référence.*
