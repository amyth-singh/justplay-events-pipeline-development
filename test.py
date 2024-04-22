import os
import pandas as pd
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
import shutil
import yaml

class CSVHandler(FileSystemEventHandler):
    def __init__(self, input_folder, output_folder, failed_folder, schema_file):
        super().__init__()
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.failed_folder = failed_folder
        self.schema = self.load_schema(schema_file)
        self.output_csv_success = os.path.join(output_folder, "output_csv_success")  # Define output_csv_success folder

    def load_schema(self, schema_file):
        with open(schema_file, 'r') as f:
            schema = yaml.safe_load(f)
        return list(schema.keys())

    def process_csv(self, csv_file_path):
        start_time = time.time()
        try:
            df = pd.read_csv(csv_file_path, delimiter=';')
            df.columns = df.columns.str.strip().str.lower().str.replace(';', ',')
            file_name = Path(csv_file_path).stem

            # Check if CSV column names match schema
            csv_columns = df.columns.tolist()
            schema_columns = self.schema
            if not set(csv_columns) == set(schema_columns):
                raise Exception("Column names don't match schema")

            output_parquet_path = os.path.join(self.output_folder, f"{file_name}.parquet")
            df.to_parquet(output_parquet_path)
            csv_size = os.path.getsize(csv_file_path)
            parquet_size = os.path.getsize(output_parquet_path)
            execution_time = time.time() - start_time
            print(f"Success: Converted {csv_file_path} ({self.get_file_size_string(csv_size)}) to {output_parquet_path} ({self.get_file_size_string(parquet_size)}), Execution Time: {execution_time:.2f} seconds")
            
            # Move the CSV file to the output_csv_success folder and append "success" to the filename
            success_file_name = f"{Path(csv_file_path).stem}_success.csv"
            shutil.move(csv_file_path, os.path.join(self.output_csv_success, success_file_name))
        except Exception as e:
            print(f"Failed to convert {csv_file_path}: {e}")
            file_name = f"{Path(csv_file_path).stem}_failed.csv"
            shutil.move(csv_file_path, os.path.join(self.failed_folder, file_name))

    def get_file_size_string(self, size_in_bytes):
        if size_in_bytes < 1024:
            return f"{size_in_bytes} B"
        elif size_in_bytes < 1024 ** 2:
            return f"{size_in_bytes / 1024:.2f} KB"
        elif size_in_bytes < 1024 ** 3:
            return f"{size_in_bytes / (1024 ** 2):.2f} MB"
        else:
            return f"{size_in_bytes / (1024 ** 3):.2f} GB"

    def on_created(self, event):
        if event.is_directory:
            return
        file_path = event.src_path
        if not file_path.endswith('.csv'):
            print(f"Failed: File {file_path} is not in CSV format")
            shutil.move(file_path, os.path.join(self.failed_folder, os.path.basename(file_path)))
        else:
            self.process_csv(file_path)

def watch_input_folder(input_folder, output_folder, failed_folder, schema_file):
    handler = CSVHandler(input_folder, output_folder, failed_folder, schema_file)
    observer = Observer()
    observer.schedule(handler, path=input_folder, recursive=False)
    observer.start()
    print("Drop CSVs into Input folder :")
    try:
        while True:
            pass
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

input_folder = "input_csv"
output_folder = "output_parquet_success"
failed_folder = "output_failed"
schema_file = "schema.yaml"
watch_input_folder(input_folder, output_folder, failed_folder, schema_file)
