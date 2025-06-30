# Pipeline Révisé - Analyse de l'Impact des Catastrophes Naturelles sur les Exportations

Ce pipeline réimplémente et étend l'analyse économétrique en suivant strictement la méthodologie de l'article de référence (`article_latex.tex`) et en incorporant les meilleures pratiques de `processing.py`.

## 🎯 Objectifs et Améliorations

### Problèmes Résolus
- ✅ **Données GeoMet manquantes** : Intégration des données d'intensité physique
- ✅ **Analyse post-2000 manquante** : Extension à la période 2000-2024  
- ✅ **Structure panel incorrecte** : Implémentation du panel produit-pays-année
- ✅ **Cache abandonné** : Restauration du système de cache robuste
- ✅ **Format de sortie inadéquat** : Tables au format publication (Table 1, 2, 3)
- ✅ **Méthodologie non-alignée** : Strict respect de la spécification article

### Nouvelles Fonctionnalités
- 📊 **Double analyse temporelle** : 1979-2000 ET 2000-2024
- 🌍 **Sources de données multiples** : EM-DAT + GeoMet + World Bank
- 📈 **Méthodologie complète** : Effets fixes, interactions, variables log-diff
- 📄 **Outputs publication** : Tables LaTeX directement utilisables
- 🔄 **Pipeline modulaire** : Étapes indépendantes et reproductibles
- 💾 **Cache intelligent** : Évite téléchargements répétés

## 📁 Structure du Pipeline

```
pipeline_revised/
├── 00_configuration.py          # Configuration centralisée
├── 01_collect_exports_data.py   # Collecte données Comtrade
├── 02_collect_disasters_data.py # Collecte EM-DAT + GeoMet 
├── 03_create_analysis_dataset.py # Création datasets finaux
├── 04_econometric_analysis.R    # Analyse économétrique
├── run_pipeline.py              # Orchestrateur principal
└── install_r_packages.R         # Installation packages R
```

## 🔗 Détail des Entrées et Sorties de chaque Étape du Pipeline

| Étape | Script | Entrées attendues | Sorties produites | Variables clés attendues |
|-------|--------|------------------|-------------------|-------------------------|
| 1. Collecte exports | 01_collect_exports_data.py | Fichiers sources Comtrade, config.json, .env (clé API) | cache/exports_combined.pkl, CSVs intermédiaires | year, iso3, hs2, export_value |
| 2. Collecte catastrophes | 02_collect_disasters_data.py | Fichiers EM-DAT (data/emdat/), config.json | cache/disasters_combined_*.pkl, CSVs intermédiaires | year, iso3, earthquake_events, flood_events, storm_events, temp_events, earthquake_deaths, ... |
| 3. Fusion/validation | 03_validate_datasets.py | exports_combined.pkl, disasters_combined_*.pkl, données population/World Bank | datasets/econometric_dataset_*.csv | year, iso3, hs2, export_value, earthquake_events, flood_events, storm_events, temp_events, ... |
| 4. Analyse économétrique | 04_econometric_analysis.R | datasets/econometric_dataset_*.csv | results/tables/*.csv, *.tex, *.rds | Toutes les variables ci-dessus + variables d’interaction (is_poor, is_small, etc.) |

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
pip install pandas numpy loguru comtradeapicall openpyxl python-dotenv

# R packages (automatique via le pipeline)
# dplyr, readr, stringr, broom, fixest, modelsummary, xtable
```

### Configuration
1. **Clé API Comtrade** : Créer `.env` avec `COMTRADE_API_KEY=votre_cle`
2. **Données EM-DAT** : S'assurer que les fichiers Excel sont dans `data/emdat/`
3. **R installé** : Vérifier que `Rscript` est disponible

### Exécution

#### Pipeline Complet
```bash
cd /Users/emmanuel/Documents/Repos/Memoire_M2
python pipeline_revised/run_pipeline.py
```

#### Étapes Individuelles
```bash
# Étape 1 : Collecte exports
python pipeline_revised/run_pipeline.py --step 1

# Étape 2 : Collecte catastrophes  
python pipeline_revised/run_pipeline.py --step 2

# Étape 3 : Création datasets
python pipeline_revised/run_pipeline.py --step 3

# Étape 4 : Analyse économétrique
python pipeline_revised/run_pipeline.py --step 4
```

#### Options
```bash
# Forcer rechargement des données
python pipeline_revised/run_pipeline.py --force-refresh
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

### Critères d'Inclusion et Exclusions

#### Pays Inclus
- **Total période 1979-2000** : 179 pays avec codes ISO3 valides
- **Pays avec données complètes** : 50 pays avec exports ET catastrophes ET caractéristiques

#### Pays Explicitement Exclus
- **DDR** (République démocratique allemande) : État dissous en 1990
- **CSK** (Tchécoslovaquie) : État dissous en 1993  
- **ANT** (Antilles néerlandaises) : Territoire dissous en 2010
- **SCG** (Serbie-Monténégro) : État dissous en 2006
- **Pays sans données World Bank** : Exclus des analyses avec interactions

#### Critères de Classification des Pays
- **Pays Pauvres** : Classés "Low income" ou "Lower middle income" par World Bank (2016)
- **Petits Pays** : Population < 20 millions d'habitants (médiane échantillon)
- **Période de référence** : Caractéristiques moyennées sur 1979-2000

#### Critères des Catastrophes Significatives
- **Seuil de significativité** : Ratio deaths/population > médiane annuelle par type de catastrophe
- **Types analysés** : Earthquakes, Floods, Storms, Extreme temperatures uniquement
- **Critères EM-DAT** : ≥10 morts OU ≥100 affectés OU état d'urgence OU appel aide internationale

### Critères d'inclusion et exclusions

#### Pays Inclus
- **Total période 1979-2000** : 179 pays avec codes ISO3 valides
- **Pays avec données complètes** : 50 pays avec exports ET catastrophes ET caractéristiques

#### Pays Explicitement Exclus
- **DDR** (République démocratique allemande) : État dissous en 1990
- **CSK** (Tchécoslovaquie) : État dissous en 1993  
- **ANT** (Antilles néerlandaises) : Territoire dissous en 2010
- **SCG** (Serbie-Monténégro) : État dissous en 2006
- **Pays sans données World Bank** : Exclus des analyses avec interactions

#### Classification des pays
- **Pays Pauvres** : Classés "Low income" ou "Lower middle income" par World Bank (2016)
- **Petits Pays** : Population < 20 millions d'habitants (médiane échantillon)
- **Période de référence** : Caractéristiques moyennées sur 1979-2000

#### Catastrophes significatives
- **Seuil de significativité** : Ratio deaths/population > médiane annuelle par type de catastrophe
- **Types analysés** : Earthquakes, Floods, Storms, Extreme temperatures uniquement
- **Critères EM-DAT** : ≥10 morts OU ≥100 affectés OU état d'urgence OU appel aide internationale

### Variables de Catastrophes

#### EM-DAT (Human Impact)
- **Occurrence** : `ln(nombre_événements + 1)`
- **Mortalité** : `ln(nombre_morts + 1)`  
- **Événements significatifs** : Seuil médiane ratio morts/population

#### GeoMet (Physical Intensity)
- **Tremblements de terre** : Échelle de Richter
- **Inondations** : Écart précipitations vs moyenne
- **Tempêtes** : Vitesse du vent
- **Températures extrêmes** : Écart température vs moyenne
- **Index composite** : Somme pondérée par inverse écart-type

### Structure Panel
- **Niveau pays** : Pays × Année (exports agrégés)
- **Niveau produit** : Pays × Produit HS2 × Année (panel détaillé)
- **Produits agricoles** : HS codes 01-24
- **Effets fixes** : Produit×Pays + Produit×Année

## 📄 Outputs

### Fichiers Générés

#### Cache (`cache/`)
- `disasters_combined_1979_2000.pkl` : Données catastrophes période 1
- `disasters_combined_2000_2024.pkl` : Données catastrophes période 2
- `analysis_country_*.pkl` : Datasets niveau pays
- `analysis_product_*.pkl` : Datasets niveau produit

#### Résultats (`results/`)
- `table1_1979_2000.csv` : Table principale période 1979-2000
- `table1_2000_2024.csv` : Table principale période 2000-2024
- `table2_poor_*.csv` : Interactions pays pauvres
- `table3_small_*.csv` : Interactions petits pays
- `dataset_summary.csv` : Statistiques descriptives
- `econometric_models_*.rds` : Objets modèles R

#### Tables LaTeX (`memoire/tables/`)
- `table1_*.tex` : Tables formatées pour intégration directe

### Format des Tables

#### Table 1 : Effets Principaux
| Variable | All Goods (1) | All Goods (2) | Agriculture (3) | Agriculture (4) |
|----------|---------------|---------------|-----------------|-----------------|
| Earthquake occurrence | β₁ | | β₁ | |
| Flood occurrence | β₂ | | β₂ | |
| Storm occurrence | β₃ | | β₃ | |
| Temperature occurrence | β₄ | | β₄ | |
| Earthquake deaths | | β₁ | | β₁ |
| ... | | | | |

#### Table 2 : Interactions Pays Pauvres
Analyse différentielle entre pays pauvres et riches.

#### Table 3 : Interactions Petits Pays  
Analyse différentielle entre petits et grands pays.

## 🔧 Architecture Technique

### Système de Cache
- **Évite re-téléchargements** : APIs Comtrade coûteuses
- **Fichiers pickle** : Sérialisation rapide pandas
- **Cache intelligent** : Détection automatique fichiers existants
- **Force refresh** : Option pour forcer mise à jour

### Gestion d'Erreurs
- **Logging détaillé** : Niveau DEBUG pour transparence totale
- **Validation données** : Vérifications cohérence et qualité
- **Fallbacks** : Gestion gracieuse des APIs indisponibles
- **Timeouts** : Éviter blocages sur gros téléchargements

### Reproductibilité
- **Seeds fixes** : Résultats déterministes
- **Versions packages** : Requirements.txt détaillé
- **Documentation** : Chaque étape documentée
- **Validation** : Tests automatiques cohérence

## 🎯 Comparaison avec Article de Référence

### Alignement Méthodologique
| Aspect | Article | Pipeline Actuel | Pipeline Révisé |
|--------|---------|-----------------|-----------------|
| Période | 1979-2000 | 1979-2000 | 1979-2000 + 2000-2024 ✅ |
| Sources données | EM-DAT + GeoMet | EM-DAT seulement | EM-DAT + GeoMet ✅ |
| Structure panel | Produit×Pays×Année | Pays×Année | Produit×Pays×Année ✅ |
| Spécification | Log-diff + FE | OLS simple | Log-diff + FE ✅ |
| Interactions | Pauvre×Petit | Manquantes | Pauvre×Petit ✅ |
| Tables | Table 1,2,3 | Format différent | Table 1,2,3 ✅ |

### Résultats Attendus
Reproduction des résultats clés de l'article :
- **Tremblements de terre** : Effet négatif robuste tous pays
- **Inondations** : Effet négatif petits pays uniquement  
- **Tempêtes** : Pas d'effet significatif
- **Températures extrêmes** : Effet variable selon source données

## 🐛 Debugging et Maintenance

### Logs Détaillés
```bash
# Tous les logs sont visibles en temps réel
# Format : [TIMESTAMP] | [LEVEL] | [MODULE] | MESSAGE
```

### Tests Modulaires
```bash
# Test individuel de chaque module
python pipeline_revised/01_collect_exports_data.py
python pipeline_revised/02_collect_disasters_data.py
# etc.
```

### Vérification Outputs
```bash
# Vérifier fichiers générés
ls -la cache/
ls -la results/
ls -la memoire/tables/
```

## 🚦 Philosophie du pipeline (mise à jour 2025)

- **Toute la création et transformation de variables est réalisée en Python (étape 03)** : Toutes les variables logarithmiques (`ln_total_occurrence`, `ln_total_deaths`, `ln_<type>_count`), les classifications de groupes (`income_group_internal`, `size_group`) et les taux de croissance (`d_ln_population`) sont calculés dans `03_validate_datasets.py` et sauvegardés dans les datasets finaux.
- **Le script R (étape 04) ne fait qu'analyser et générer les tables** : Aucun calcul ou transformation n'est effectué dans R, qui vérifie seulement la présence des variables nécessaires et exécute les modèles et la génération des tables. Si une variable est absente, un warning est affiché.
- **Suppression de toute logique redondante ou héritée** : Toute logique de création de variables, d'affectation de groupes, etc., a été supprimée du script R. Le pipeline est strictement linéaire : Python prépare, R analyse.
- **Les tables avancées (ex-04b) sont désormais intégrées** : Toutes les tables avancées/robustes sont générées dans la boucle principale de l'étape 04, à partir des mêmes données chargées, sans boucle secondaire ni code hérité.
- **Documentation et diagnostics** : Le pipeline est robuste, avec des warnings et diagnostics clairs pour toute variable manquante, et chaque étape est documentée ici et dans le manuscrit LaTeX.

## 🔄 Chronologie du pipeline (2025)

1. **01_collect_exports_data.py** : Collecte et nettoyage des exports (aucune transformation)
2. **02_collect_disasters_data.py** : Préparation des variables de catastrophes (EM-DAT, GeoMet)
3. **03_validate_datasets.py** : Fusion, validation, calcul de toutes les variables log/groupes et taux de croissance. Génère les datasets finaux par période.
4. **04_econometric_analysis.R** : Charge les datasets, vérifie la présence des variables, exécute toutes les analyses et génère toutes les tables (y compris avancées/robustes) dans une seule boucle linéaire.

## 📝 Notes de reproductibilité

- Si des valeurs NA ou manquantes apparaissent dans les tables R, vérifier que toutes les variables log/groupes sont bien présentes dans les CSV produits par l'étape 03.
- Toute la configuration (périodes, types de catastrophes, chemins de sortie) est dans `config.json`.
- Voir aussi le pipeline/README.md et la section méthodologie du manuscrit LaTeX.

---

*Pipeline créé le 15 juin 2025 - Révision complète de l'analyse économétrique selon méthodologie article de référence.*
