import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

csv_file_path = r"C:\Users\cdani\OneDrive\Documents\CSC490\synth_dat.csv"
df = pd.read_csv(csv_file_path)
table = pa.Table.from_pandas(df)

parquet_file_path = r"C:\Users\cdani\OneDrive\Documents\CSC490\data.parquet"
pq.write_table(table, parquet_file_path)
