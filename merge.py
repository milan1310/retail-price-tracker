import pandas as pd
import glob
import os

# Specify the pattern matching for csv files
pattern = "*.csv"

# Get a list of all csv files in the directory
csv_files = glob.glob(f"*.csv")
print(csv_files)

# Create an empty list to hold dataframes
dfs = []

# Loop over csv files and append to dfs list
for csv in csv_files:
    # Check if file is not empty
    if os.stat(csv).st_size != 0:
        df = pd.read_csv(csv, index_col=None, header=0)
        dfs.append(df)
    else:
        print(f"Skipping empty file: {csv}")

# Concatenate all dataframes in the dfs list
merged_df = pd.concat(dfs, axis=0, ignore_index=True)

# Write the output to a new csv file
merged_df.to_csv("./tmp/amazonpdp_hanes_2024-05-01.csv", index=False)