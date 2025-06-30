import pandas as pd
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
CACHE_DIR = PROJECT_ROOT / "cache"
RESULTS_DIR = PROJECT_ROOT / "results"

# Find pickle files
pickle_files = list(CACHE_DIR.glob("analysis_*.pkl"))
print(f"Found {len(pickle_files)} pickle files to convert")

for pickle_file in pickle_files:
    try:
        df = pd.read_pickle(pickle_file)
        csv_file = RESULTS_DIR / (pickle_file.stem + ".csv")
        df.to_csv(csv_file, index=False)
        print(f"Exported {pickle_file.name} -> {csv_file.name} ({len(df)} rows)")
    except Exception as e:
        print(f"Failed to export {pickle_file.name}: {e}")

print("CSV export completed")
