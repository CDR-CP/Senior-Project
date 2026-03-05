from pathlib import Path

import duckdb

parquet_path = Path(__file__).parent / "data.parquet"

con = duckdb.connect()
print("Reading first 10 rows of parquet")
result = con.execute(
    f"SELECT * FROM read_parquet('{parquet_path}') LIMIT 10;"
).fetchall()
for row in result:
    print(row)
count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_path}');").fetchone()
print("Total rows:", count[0] if count else "N/A")
con.close()
