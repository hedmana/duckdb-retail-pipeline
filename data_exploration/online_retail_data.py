#!/usr/bin/env python3
"""
Data exploration script for online retail Excel data.
Examines sheet structure and data characteristics.
"""

import pandas as pd

sheets = pd.read_excel('data/raw/online_retail_II.xlsx',
                       sheet_name=None, engine='openpyxl')
print('Available sheets:', list(sheets.keys()))
for name, df in sheets.items():
    print(f'\n{name}: {df.shape[0]:,} rows, {df.shape[1]} columns')
    print('Columns:', list(df.columns))
    print(f'\n=== {name} Details ===')
    print(
        f'Date range: {df["InvoiceDate"].min()} to {df["InvoiceDate"].max()}')
    print(f'Countries: {df["Country"].nunique()} unique')
    print(
        f'Customers: {df["Customer ID"].nunique()} unique (nulls: {df["Customer ID"].isnull().sum()})')
    print(
        f'Cancellations: {df[df["Invoice"].str.startswith("C", na=False)].shape[0]} rows')
    print('\nSample data:')
    print(df.head(3))
