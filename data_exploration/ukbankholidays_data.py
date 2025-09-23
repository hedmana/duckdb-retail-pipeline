import pandas as pd

# Read the holidays file
df = pd.read_excel('data/raw/ukbankholidays-jul19.xls')

print("=== UK Bank Holidays ===")
print(f"Shape: {df.shape}")
print(f"Columns: {list(df.columns)}")
print(f"\nSample data:")
print(df.head())
print(f"\nDate range: {df.iloc[:, 0].min()} to {df.iloc[:, 0].max()}")
print(f"Data types:\n{df.dtypes}")
