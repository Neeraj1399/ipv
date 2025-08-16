import pandas as pd
from pathlib import Path
from typing import Tuple, Dict, Union
import re
import glob
import os

def load_and_normalize(path: Union[str, Path]) -> Tuple[pd.DataFrame, Dict]:
    path = Path(path)

    # If the path is a file, use its parent folder
    if path.is_file():
        folder_path = path.parent
    else:
        folder_path = path

    # Find all maker CSV files
    all_files = glob.glob(str(folder_path / "maker *.csv"))
    if not all_files:
        raise FileNotFoundError(f"No CSV files found matching 'maker *.csv' in {folder_path}")

    df_list = []
    for file in all_files:
        try:
            # Read CSV, allowing extra commas
            temp_df = pd.read_csv(file, dtype=str, on_bad_lines='skip')
        except TypeError:
            # Fallback for pandas < 1.3
            temp_df = pd.read_csv(file, dtype=str, error_bad_lines=False)

        # Extract year from filename if not in CSV
        year_from_name = re.search(r'maker (\d{4})', os.path.basename(file), re.IGNORECASE)
        if year_from_name:
            temp_df['year'] = int(year_from_name.group(1))

        # Standardize column names
        temp_df.columns = [c.strip().lower() for c in temp_df.columns]

        # Rename columns to standard format
        if 'total' in temp_df.columns:
            temp_df = temp_df.rename(columns={'total': 'registrations'})
        if 'maker' in temp_df.columns:
            temp_df = temp_df.rename(columns={'maker': 'manufacturer'})

        # Ensure year is numeric
        temp_df['year'] = pd.to_numeric(temp_df['year'], errors='coerce')

        # Add period column (start of year)
        temp_df['period'] = pd.to_datetime(temp_df['year'].astype('Int64').astype(str) + '-01-01', errors='coerce')

        # Clean registrations
        if 'registrations' in temp_df.columns:
            temp_df['registrations'] = pd.to_numeric(
                temp_df['registrations'].astype(str).str.replace(',', '').str.strip(),
                errors='coerce'
            ).fillna(0).astype(float)
        else:
            temp_df['registrations'] = 0.0

        # Map vehicle category
        def map_category(x: str) -> str:
            s = str(x).lower()
            if 'two' in s or '2w' in s:
                return '2W'
            if 'three' in s or '3w' in s or 'auto' in s:
                return '3W'
            if 'four' in s or '4w' in s or 'car' in s or 'jeep' in s:
                return '4W'
            return 'OTHER'

        if 'vehicle_category' in temp_df.columns:
            temp_df['vehicle_category'] = temp_df['vehicle_category'].apply(map_category)
        else:
            temp_df['vehicle_category'] = 'OTHER'

        # Keep only expected columns
        df_list.append(temp_df[['period', 'year', 'vehicle_category', 'manufacturer', 'registrations']])

    # Combine all data
    df = pd.concat(df_list, ignore_index=True)
    metadata = {'granularity': 'yearly'}

    return df, metadata
