import pandas as pd
import glob
import os

# 1. Identify all your excel files
folder_path = r"C:\\Users\\Nikhil.Jain\\Downloads\\Tariff Database - All Countries - August Update (3)\\Tariff Database - All Countries - August Update"
excel_files = glob.glob(os.path.join(folder_path, "*.xlsx")) # Adjust this if your files are in a specific folder

all_dfs = []
all_columns = set()

print("Step 1: Analyzing headers across all files...")
for file in excel_files:
    # Read only the first row to get column names (saves memory)
    columns = pd.read_excel(file,header=2,sheet_name="Master Sheet",nrows=0).columns
    all_columns.update(columns)
    print(f"Found {len(columns)} columns in {file}")

print(f"Total unique columns identified: {len(all_columns)}")

# 2. Process and Merge
print("\nStep 2: Merging data (this may take a few minutes)...")
final_data_list = []

for file in excel_files:
    print(f"Processing {file}...")
    df = pd.read_excel(file,sheet_name="Master Sheet",header=2)
    
    # Reindex: This magic line adds missing columns as 'NaN' 
    # so every file matches the 'Master' structure
    df = df.reindex(columns=list(all_columns))
    
    # Fill the missing columns for this specific file with 'N/A'
    df.fillna("N/A", inplace=True)
    
    final_data_list.append(df)

# 3. Concatenate everything into one massive dataframe

master_df = pd.concat(final_data_list, ignore_index=True)

# 4. Save to Parquet (The format required for the high-speed app)
output_filename = "master_data.parquet"
master_df= master_df.astype(str)
master_df.to_parquet(output_filename, index=False, engine='pyarrow')

print(f"\nSuccess! Created '{output_filename}'")
print(f"Total Rows: {len(master_df)}")
print(f"Total Columns: {len(master_df.columns)}")