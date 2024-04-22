import os
import time

def timeit(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = round(end_time - start_time, 6)
        print(f"Execution time for {func.__name__}: {execution_time} seconds")
        return result
    return wrapper

# def show_file_sizes(func):
#     def wrapper(csv_file):
#         csv_size = os.path.getsize(csv_file)
#         parquet_file = func(csv_file)
#         parquet_size = os.path.getsize(parquet_file)
#         print(f"CSV file size: {csv_size} bytes")
#         print(f"Parquet file size: {parquet_size} bytes")
#     return wrapper
