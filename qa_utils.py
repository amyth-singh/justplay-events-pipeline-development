import os
import time

def timeit(func):
    """
    Decorator to measure the execution time of a function.
    Prints the execution time in seconds.
    """
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = round(end_time - start_time, 6)
        print(f"Execution time for {func.__name__}: {execution_time} seconds")
        return result
    return wrapper

def file_size(func):
    """
    Decorator to measure the csv and parquet filesize.
    Prints the size in mb.
    """
    def wrapper(input_file):
        def get_file_size_mb(file_path):
            return round(os.path.getsize(file_path) / (1024 * 1024), 2)
        csv_size_before = get_file_size_mb(input_file)
        func(input_file)
        output_file = input_file.replace('.csv', '_temp.csv')
        parquet_file = input_file.replace('.csv', '.parquet')
        parquet_size_after = get_file_size_mb(parquet_file)
        print(f"CSV file size: {csv_size_before} MB")
        print(f"Parquet file size: {parquet_size_after} MB")
    return wrapper