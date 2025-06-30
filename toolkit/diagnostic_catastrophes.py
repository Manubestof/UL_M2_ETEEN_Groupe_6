import pandas as pd

print('--- DIAGNOSTIC VARIABLES CATASTROPHES ---')
df = pd.read_csv('datasets/econometric_dataset_1979_2000.csv')
for col in df.columns:
    if any(x in col for x in ['deaths','events','affected','intensity','index']):
        n_nonzero = (df[col]!=0).sum()
        print(f'{col:40s}  non-zero: {n_nonzero}')
