import duckdb 

con = duckdb.connect()

# parquet_path = r"C:\Users\cdani\OneDrive\Documents\CSC490\data.parquet"
parquet_path = r"C:\Users\denni\PycharmProjects\Senior Project\Senior-Project\data.parquet"

print("Reading first 10 rows of parquet")
result = con.execute(f"SELECT * FROM read_parquet('{parquet_path}') LIMIT 10;").fetchall()
for row in result:
    print(row)

count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_path}');").fetchone()
print("Total rows:", count[0])


con.close()