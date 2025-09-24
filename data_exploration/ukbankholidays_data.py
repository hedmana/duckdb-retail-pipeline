#!/usr/bin/env python3
"""
Data exploration script for UK bank holidays Excel file.
Examines holiday data structure and date ranges.
"""

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
