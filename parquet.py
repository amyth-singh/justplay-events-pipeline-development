import pandas as pd
import os

def open_parquet(parquet_file):
    """
    Fuction checks if the parquet file is created with the right formatting.
    Prints Top 5 Rows if exists.
    """
    if not os.path.exists(parquet_file):
        print(f"Parquet file '{parquet_file}' does not exist.")
        return None

    df = pd.read_parquet(parquet_file)
    return df

parquet_df = open_parquet("C:\\Users\\amith\\Desktop\\code_elt\\justplay-infra-pipeline-development\\output\\student-mat.parquet")
if parquet_df is not None:
    print(parquet_df.head(5))
