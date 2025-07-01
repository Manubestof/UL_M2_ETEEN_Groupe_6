import pandas as pd
import glob
import os

files = glob.glob('datasets/econometric_dataset_*.csv')
for f in files:
    df = pd.read_csv(f)
    agri = df[df['is_agri']==True] if 'is_agri' in df.columns else pd.DataFrame()
    print(f'\n==== {os.path.basename(f)} ====')
    for col in ['ln_total_occurrence','ln_total_deaths','disaster_index']:
        if col in agri.columns:
            n_na = agri[col].isna().sum()
            n_tot = len(agri)
            print(f"{col}: {n_tot-n_na}/{n_tot} non-NA ({n_na} NA)")
        else:
            print(f"{col}: MISSING")
    # Affiche un aperçu des premières lignes agri pour debug
    if not agri.empty:
        print("Aperçu lignes agri:")
        print(agri.head(3).to_string(index=False))
