import os
import pandas as pd
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from qa_utils import timeit

@timeit
def convert_csv_to_parquet(input_file, output_directory):
    if not os.path.exists(input_file):
        print(f"Input file '{input_file}' does not exist.")
        return
    output_file = input_file.replace('.csv', '_temp.csv')
    with open(input_file, 'r') as f_in, open(output_file, 'w') as f_out:
        f_out.write(f_in.read().replace(';', ','))
    parquet_file = os.path.join(output_directory, os.path.basename(input_file).replace('.csv', '.parquet'))
    if os.path.exists(parquet_file):
        print(f"Parquet file '{parquet_file}' already exists. Conversion skipped.")
    else:
        pd.read_csv(output_file).to_parquet(parquet_file, index=False)
        print(f"Conversion from CSV to Parquet completed successfully.")
    os.remove(output_file)

class CSVHandler(FileSystemEventHandler):
    def __init__(self, output_directory):
        super().__init__()
        self.output_directory = output_directory
    def on_created(self, event):
        if event.is_directory: return
        name, ext = os.path.splitext(event.src_path)
        if ext == '.csv' and not name.endswith('_temp'):
            print(f"Detected new CSV file: {event.src_path}")
            convert_csv_to_parquet(event.src_path, self.output_directory)

def monitor_directory(input_directory, output_directory):
    event_handler = CSVHandler(output_directory)
    observer = Observer()
    observer.schedule(event_handler, input_directory, recursive=True)
    observer.start()
    print(f"Monitoring directory '{input_directory}' for new CSV files...")
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        observer.join()

if __name__ == "__main__":
    monitor_directory(
        "C:\\Users\\amith\\Desktop\\code_elt\\justplay-infra-pipeline-development\\input_csv_drop",
        "C:\\Users\\amith\\Desktop\\code_elt\\justplay-infra-pipeline-development\\output_parquet"
    )
