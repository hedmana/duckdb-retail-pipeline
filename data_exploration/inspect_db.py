"""Quick database inspection script."""

import duckdb

# Connect to your database
conn = duckdb.connect('build/retail.duckdb')

print("=== TABLES IN DATABASE ===")
tables = conn.execute("SHOW TABLES").fetchall()
for (table_name,) in tables:
    print(f"- {table_name}")

print("\n=== TABLE DETAILS ===")
for (table_name,) in tables:
    row_count = conn.execute(
        f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    columns = conn.execute(f"DESCRIBE {table_name}").fetchall()
    print(f"\n{table_name}: {row_count:,} rows")
    print("Columns:")
    for col_name, col_type, null, key, default, extra in columns:
        print(f"  - {col_name}: {col_type}")

conn.close()
