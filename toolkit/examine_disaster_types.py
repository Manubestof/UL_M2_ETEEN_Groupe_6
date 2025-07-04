#!/usr/bin/env python3
"""
Script pour examiner tous les types de catastrophes disponibles dans EM-DAT et GeoMet
"""

import pandas as pd
import numpy as np
from pathlib import Path

# Chemins des 
emdat_1979_2000 = Path("../data/emdat/EM-DAT 1979-2000.xlsx")
emdat_2000_plus = Path("../data/emdat/EM-DAT countries 2000+.xlsx")
geomet_csv = Path("../data/geomet/geomet.csv")

print("=" * 80)
print("EXAMEN DES TYPES DE CATASTROPHES DANS EM-DAT ET GEOMET")
print("=" * 80)

# EM-DAT 1979-2000
print("\n1. EM-DAT 1979-2000 (structure événement par ligne)")
print("-" * 50)
try:
    df_emdat_old = pd.read_excel(emdat_1979_2000, sheet_name="EM-DAT Data")
    print(f"Nombre de lignes : {len(df_emdat_old):,}")
    print(f"Colonnes disponibles : {list(df_emdat_old.columns)}")
    
    # Types de catastrophes
    if "Disaster Type" in df_emdat_old.columns:
        disaster_types = df_emdat_old["Disaster Type"].value_counts()
        print(f"\nTypes de catastrophes (top 20) :")
        print(disaster_types.head(20))
        print(f"\nNombre total de types uniques : {len(disaster_types)}")
    
    # Groupes de catastrophes
    if "Disaster Group" in df_emdat_old.columns:
        disaster_groups = df_emdat_old["Disaster Group"].value_counts()
        print(f"\nGroupes de catastrophes :")
        print(disaster_groups)
    
    # Sous-groupes
    if "Disaster Subgroup" in df_emdat_old.columns:
        disaster_subgroups = df_emdat_old["Disaster Subgroup"].value_counts()
        print(f"\nSous-groupes de catastrophes (top 15) :")
        print(disaster_subgroups.head(15))
        
    # Sous-types
    if "Disaster Subtype" in df_emdat_old.columns:
        disaster_subtypes = df_emdat_old["Disaster Subtype"].value_counts()
        print(f"\nSous-types de catastrophes (top 15) :")
        print(disaster_subtypes.head(15))
        
except Exception as e:
    print(f"Erreur lecture EM-DAT 1979-2000 : {e}")

# EM-DAT 2000+
print("\n\n2. EM-DAT 2000+ (structure agrégée)")
print("-" * 50)
try:
    df_emdat_new = pd.read_excel(emdat_2000_plus, skiprows=[1])
    print(f"Nombre de lignes : {len(df_emdat_new):,}")
    print(f"Colonnes disponibles : {list(df_emdat_new.columns)}")
    
    # Types de catastrophes
    if "Disaster Type" in df_emdat_new.columns:
        disaster_types_new = df_emdat_new["Disaster Type"].value_counts()
        print(f"\nTypes de catastrophes (top 20) :")
        print(disaster_types_new.head(20))
        print(f"\nNombre total de types uniques : {len(disaster_types_new)}")
    
    # Groupes de catastrophes
    if "Disaster Group" in df_emdat_new.columns:
        disaster_groups_new = df_emdat_new["Disaster Group"].value_counts()
        print(f"\nGroupes de catastrophes :")
        print(disaster_groups_new)
        
    # Sous-groupes
    if "Disaster Subroup" in df_emdat_new.columns:  # Note: typo dans le fichier original
        disaster_subgroups_new = df_emdat_new["Disaster Subroup"].value_counts()
        print(f"\nSous-groupes de catastrophes (top 15) :")
        print(disaster_subgroups_new.head(15))
        
    # Sous-types
    if "Disaster Subtype" in df_emdat_new.columns:
        disaster_subtypes_new = df_emdat_new["Disaster Subtype"].value_counts()
        print(f"\nSous-types de catastrophes (top 15) :")
        print(disaster_subtypes_new.head(15))
        
except Exception as e:
    print(f"Erreur lecture EM-DAT 2000+ : {e}")

# GeoMet
print("\n\n3. GeoMet (intensité physique)")
print("-" * 50)
try:
    df_geomet = pd.read_csv(geomet_csv)
    print(f"Nombre de lignes : {len(df_geomet):,}")
    print(f"Colonnes disponibles : {list(df_geomet.columns)}")
    
    # Types de phénomènes dans GeoMet (identifiés par les suffixes des colonnes)
    print(f"\nTypes de phénomènes identifiés dans GeoMet :")
    
    # Chercher les suffixes après les underscores
    suffixes = set()
    for col in df_geomet.columns:
        if "_" in col and ("killed_pop_" in col or "affected_pop_" in col or "damage_gdp_" in col):
            suffix = col.split("_")[-1]
            suffixes.add(suffix)
    
    print(f"Suffixes identifiés : {sorted(suffixes)}")
    
    # Mapping probable des suffixes
    suffix_mapping = {
        'eq': 'Earthquake (tremblements de terre)',
        'vol': 'Volcanic activity (activité volcanique)', 
        'str': 'Storm (tempêtes)',
        'temp': 'Temperature extreme (températures extrêmes)',
        'fld': 'Flood (inondations)',
        'drg': 'Drought (sécheresses)'
    }
    
    print(f"\nMapping probable des types GeoMet :")
    for suffix in sorted(suffixes):
        description = suffix_mapping.get(suffix, f"Type inconnu ({suffix})")
        print(f"  {suffix} : {description}")
        
        # Compter les observations non-nulles pour chaque type
        killed_col = f"killed_pop_{suffix}"
        affected_col = f"affected_pop_{suffix}"
        damage_col = f"damage_gdp_{suffix}"
        
        if killed_col in df_geomet.columns:
            non_null_killed = df_geomet[killed_col].notna().sum()
            print(f"    - Observations avec morts non-nulles : {non_null_killed:,}")
        if affected_col in df_geomet.columns:
            non_null_affected = df_geomet[affected_col].notna().sum()
            print(f"    - Observations avec affectés non-nulles : {non_null_affected:,}")
        if damage_col in df_geomet.columns:
            non_null_damage = df_geomet[damage_col].notna().sum()
            print(f"    - Observations avec dégâts non-nulles : {non_null_damage:,}")
            
except Exception as e:
    print(f"Erreur lecture GeoMet : {e}")

print("\n" + "=" * 80)
print("ANALYSE TERMINÉE")
print("=" * 80)
