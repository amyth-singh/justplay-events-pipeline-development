#%%
import os
import polars as pl
import pyarrow.csv as csv
import pyarrow.parquet as pq
from testing import timeit

@timeit
def csv_to_parquet(csv_file):
    csv_file_name = os.path.basename(csv_file)
    parquet_file = os.path.splitext(csv_file_name)[0] + ".parquet"
    
    # Check if Parquet file already exists
    if os.path.exists(parquet_file):
        print(f"Parquet file '{parquet_file}' already exists. Conversion aborted.")
        return
    
    table = csv.read_csv(csv_file)
    pq.write_table(table, parquet_file)
    print(f"'{csv_file}' converted to '{parquet_file}'")



csv_file = 'student-mat.csv'
parquet_file = csv_file
csv_to_parquet(csv_file, )