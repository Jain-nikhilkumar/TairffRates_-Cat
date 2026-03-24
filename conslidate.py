
import pandas as pd
import glob
import os

# 1. CONFIGURATION
folder_path = r"C:\\Users\\Nikhil.Jain\\Downloads\\Tariff Database - All Countries - August Update2 (1)\\Tariff Database - All Countries - August Update"
excel_files = glob.glob(os.path.join(folder_path, "*.xlsx"))

# Define columns that MUST be numeric (decimals/amounts)
numeric_cols = [
    'Old Tariff (Dec 2024)',
    'Sec 232 Tariffs',
    'Sec 232 Copper',
    'Mexican Tomatoes (in effect from 14-Jul-2025)',
    'Canadian Energy',
    'Canadian & Mexican Potash',
    'IEEPA Tariffs on China',
    'IEEPA Tariffs on Mexico & Canada',
    'Reciprocal Tariff',
    '90 Day Pause on All others',
    'Revised Reciprocal Tariffs',
    'Electronics Exemptions',
    'Annex II Exemptions',
    'If Reciprocal Tariff were applied',
    'Tariff as of 10-April-2025',
    'Revised Reciprocal Tariffs (To go in effect on 01-Aug-2025)',
    'US Import $',
    '2024 Imports for Consumption',
    'Total Tariff Paid On Dec 2024',
    'Total Tariff Paid On Recoprocal Tariff',
    'Total Tariff Paid On 10-April-2025'
]

all_data = []
all_columns = set()

print("Step 1: Identifying all unique columns...")
for file in excel_files:
    cols = pd.read_excel(file, sheet_name="Master Sheet", header=2, nrows=0).columns
    all_columns.update(cols)

# 2. MERGE LOGIC
print(f"Step 2: Processing {len(excel_files)} files...")
for file in excel_files:
    print(f"-> Reading: {os.path.basename(file)}")
    df = pd.read_excel(file, sheet_name="Master Sheet", header=2)
    
    # Align to master column list
    df = df.reindex(columns=list(all_columns))

    # Apply Type Safety per column
    for col in df.columns:
        if col in numeric_cols:
            # Convert to number, invalid becomes NaN, then fill with 0.0
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
        else:
            # Convert to string, fill empty with N/A
            df[col] = df[col].astype(str).replace(['nan', 'None', 'NAT'], 'N/A').fillna('N/A')
    
    all_data.append(df)

# 3. CONSOLIDATE & SAVE
print("Step 3: Finalizing Parquet Database...")
master_df = pd.concat(all_data, ignore_index=True)

# Ensure HS Code is always treated as a string to keep leading zeros
if 'HS Code' in master_df.columns:
    master_df['HS Code'] = master_df['HS Code'].astype(str)

master_df.to_parquet("master_data.parquet", index=False, engine='pyarrow')

print(f"\nSUCCESS! Created 'master_data.parquet'")
print(f"Total Rows: {len(master_df)} | Total Columns: {len(master_df.columns)}")

