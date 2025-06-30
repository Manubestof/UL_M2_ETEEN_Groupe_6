# Modular Econometric Pipeline – Natural Disasters and Exports

This repository contains a modular, fully documented pipeline for collecting, merging, and preparing export (Comtrade) and disaster (EM-DAT, GeoMet) data for econometric analysis. The pipeline is designed for reusability and transparency, with all configuration (paths, periods, thresholds, etc.) centralized in `config.json`.

## 📁 Repository Structure

```
├── pipeline/              # Complete econometric pipeline
│   ├── 01_collect_exports_data.py  # UN Comtrade data collection
│   ├── 02_collect_disasters_data.py  # EM-DAT & GeoMet data collection
│   ├── 03_validate_datasets.py  # Dataset preparation (per period)
│   ├── 04_econometric_analysis.R      # R econometric analysis
│   ├── config.json         # All paths, periods, and parameters
│   └── utils/              # Utility functions
├── datasets/              # Output econometric datasets (CSV, per period)
├── results/               # Analysis results (CSV only)
├── results/tables/        # Output tables (CSV only)
├── requirements.txt       # Python dependencies
```

## 🚀 Quick Start

### Prerequisites

1. **Python 3.8+** with packages from `requirements.txt`
2. **R 4.0+** with packages: `plm`, `sandwich`, `lmtest`
3. **[Optional] UN Comtrade API Key** (free registration) - you can also download data directly

### Setup

1. Clone the repository:
```bash
git clone https://github.com/Manubestof/Memoire_M2.git
cd UL_M2_ETEEN_Groupe_6
```

2. Install Python dependencies:
```bash
pip install -r requirements.txt
```

3. Configure your API key and parameters in `.env`.

4. Run the entire pipeline or step by step
```bash
cd pipeline
python run_pipeline.py
```

```bash
cd pipeline
python 01_collect_exports_data.py --fetch-missing --clear-cache
python 02_collect_disasters_data.py
python 03_validate_datasets.py
# Then run the R analysis for each period:
Rscript 04_econometric_analysis.R
```
Arguments:
- `--fetch-missing`: Enables the logic to fetch only missing years from the API (default: enabled).
- `--clear-cache`: Forces deletion of all cached export data before fetching (useful for a clean run).
- Both options are available as CLI arguments and/or in `config.json`.

## 📊 Outputs

- **Econometric Datasets**: One CSV file per period, e.g. `datasets/econometric_dataset_1979_2000.csv`, `datasets/econometric_dataset_2000_2024.csv`.
- **Results and Tables**: All outputs are CSV files in `results/` and `results/tables/`

## 🧩 Pipeline Logic

- **Modular Steps**: Each script is independent and uses only parameters/paths from `config.json`.
- **Per-Period Output**: The pipeline produces one econometric dataset per period, never a single merged file.
- **Dynamic File Management**: All output directories and filenames are defined in `config.json` and created dynamically.
- **No Hardcoded Paths**: All scripts use project-root-relative paths from `config.json`.

## 🔍 Fetch-Missing Logic

- **Detection**: Before fetching export data, the pipeline scans the local export files and logs which years are already present (including multi-year files).
- **Missing Years**: Only years not present locally are fetched from the Comtrade API.
- **Logging**: The pipeline logs all years present and missing, and fetches only the missing years.
- **No np.int64 Artifacts**: All year values in logs are standard Python integers for clarity.

## 📚 Data Sources

- **EM-DAT:**
    - [1979–2000](https://public.emdat.be/data)
    - [⩾2000](https://data.humdata.org/dataset/emdat-country-profiles)
- [**GeoMet**](https://www.ifo.de/ebdc-datensaetze/ifo-game-die-geologique-und-meteorologische-event-datenbank)
- [**Trade flow (UN Comtrade)**](https://comtradeplus.un.org/TradeFlow)
- [**Income groups:** World Bank Country and Lending Groups](https://datahelpdesk.worldbank.org/knowledgebase/articles/906519-world-bank-country-and-lending-groups)
- [**Population:** United Nations, Department of Economic and Social Affairs, Population Division](https://population.un.org/wpp/downloads?folder=Standard%20Projections&group=Most%20used)

## 🔄 Reproducibility

- **Caching System**: Efficient data reuse and incremental updates
- **Version Control**: All code and methodology documented
- **Automatic Testing**: Pipeline validation and conformity checks
- **Documentation**: Complete README and inline documentation

## 📄 Citation

If you use this pipeline, please cite:

```bibtex
@misc{Effet des catasrophes naturelles sur les exports internationaux,
  title={Modular Pipeline for Natural Disasters and Exports Analysis},
  author={Emmanuel Le Hir, Diane Varichon, Stéphane Nzépa, Arthur Berthod},
  year={2025},
  url={https://github.com/Manubestof/UL_M2_ETEEN_Groupe_6}
}
```

## 📞 Contact

For questions or issues:
- Create an issue on GitHub
- Check the pipeline documentation in `pipeline/README.md`

## ⚖️ License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🚦 Pipeline Philosophy

- **All variable creation and transformation is performed in Python (step 03)**: All log variables (e.g. `ln_total_occurrence`, `ln_total_deaths`, `ln_<type>_count`), group classifications (`income_group_internal`, `size_group`), and growth rates (`d_ln_population`) are computed in `03_validate_datasets.py` and saved in the final dataset.
- **R script (step 04) only performs analysis and table generation**: The R script does not create or transform variables, but checks for their presence and runs the econometric models and table outputs. If a variable is missing, a warning is issued.
- **No redundant or legacy logic**: All legacy or redundant logic (variable creation, group assignment, etc.) has been removed from the R script. The pipeline is strictly linear: Python prepares, R analyzes.
- **Advanced tables are fully integrated**: All advanced/robustness tables are generated in the main period loop in step 04, using the same loaded data, with no secondary loops or legacy code.
- **Documentation and diagnostics**: The pipeline is robust, with clear warnings and diagnostics for missing variables, and all steps are documented in this README and in the LaTeX manuscript.

## 🔄 Pipeline Chronology

1. **01_collect_exports_data.py**: Collects and cleans export data (no transformation logic)
2. **02_collect_disasters_data.py**: Prepares disaster variables (EM-DAT, GeoMet)
3. **03_validate_datasets.py**: Merges, validates, and computes all log/group variables and growth rates. Outputs final econometric datasets (per period).
4. **04_econometric_analysis.R**: Loads datasets, checks for required variables, runs all analyses and generates all tables (including advanced/robustness tables) in a single, linear loop.

## 📝 Notes for Reproducibility

- If you encounter NA or missing values in R output tables, check that all log/group variables are present in the CSVs produced by step 03.
- All configuration (periods, disaster types, output paths) is in `config.json`.
- For more details, see the pipeline/README.md and the methodology section in the LaTeX manuscript.
