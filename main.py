import os
import time
import pandas as pd
import logging
import yaml
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime

class CSVHandler(FileSystemEventHandler):
    csv_processing_times, parquet_processing_times = [], []
    total_csv_size, total_parquet_size = 0, 0
    total_parquet_files = 0  # New counter for total Parquet files converted

    def __init__(self, input_folder, output_folder, schema_file):
        super().__init__()
        self.counter = 0
        self.input_folder, self.output_folder, self.schema_file = input_folder, output_folder, schema_file
        self.schema = self.load_schema(schema_file)

    def load_schema(self, schema_file):
        with open(schema_file, 'r') as f:
            return yaml.safe_load(f)

    def validate_schema(self, df):
        return set(df.columns) == set(self.schema.keys())

    def on_created(self, event):
        if event.is_directory or not event.src_path.endswith('.csv'):
            return

        start_time_csv = time.time()
        try:
            df = pd.read_csv(event.src_path, delimiter=';').dropna()
        except Exception as e:
            logging.error(f"Error processing file: {event.src_path}. {e}")
            return

        df.columns = map(str.lower, df.columns)
        df = df.map(lambda x: str(x).lower() if isinstance(x, str) else str(x))
        
        df['creating_timestamp'] = datetime.now()
        df['id'] = range(self.counter, self.counter + len(df))
        self.counter += len(df)

        if not self.validate_schema(df):
            logging.error(f"CSV schema does not match schema.yaml for file: {event.src_path}")
            print(f"Error: CSV schema does not match schema.yaml for file: {event.src_path}. Conversion halted.")
            self.move_to_failed(event.src_path)
            return

        parquet_filename = os.path.join(self.output_folder, f"{os.path.splitext(os.path.basename(event.src_path))[0]}.parquet")
        
        start_time_parquet = time.time()
        df.to_parquet(parquet_filename)
        end_time_parquet = time.time()
        
        parquet_processing_time = end_time_parquet - start_time_parquet
        self.parquet_processing_times.append(parquet_processing_time)
        
        csv_size = os.path.getsize(event.src_path)
        self.total_csv_size += csv_size

        logging.info(f"Converted {event.src_path} to Parquet format")
        print(f"Conversion successful: {event.src_path} uploaded to {parquet_filename}")
        self.delete_csv(event.src_path)

        CSVHandler.total_parquet_files += 1  # Increment the counter for each converted Parquet file

    def move_to_failed(self, csv_file):
        failed_folder = os.path.join(os.path.dirname(csv_file), 'output_failed')
        os.makedirs(failed_folder, exist_ok=True)
        dest = os.path.join(failed_folder, os.path.basename(csv_file))
        os.rename(csv_file, dest)
        logging.info(f"Moved unsuccessful CSV file {csv_file} to output_failed folder")

    @staticmethod
    def delete_csv(csv_file):
        os.remove(csv_file)
        logging.info(f"Deleted CSV file {csv_file}")

def watch_input_csv_folder(input_folder, output_folder, schema_file):
    observer = Observer()
    observer.schedule(CSVHandler(input_folder, output_folder, schema_file), path=input_folder)
    observer.start()
    logging.info("Watching input CSV folder...")
    print("Watching input CSV folder...")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        observer.join()

if __name__ == "__main__":
    input_csv_folder = 'input_csv'
    output_parquet_folder = 'output_parquet'
    schema_file = 'schema.yaml'

    os.makedirs(output_parquet_folder, exist_ok=True)
    logging.basicConfig(filename='conversion_log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    watch_input_csv_folder(input_csv_folder, output_parquet_folder, schema_file)

    total_parquet_size = sum(os.path.getsize(os.path.join(output_parquet_folder, f)) for f in os.listdir(output_parquet_folder) if f.endswith('.parquet'))

    total_parquet_size_mb = total_parquet_size / (1024 * 1024)

    logging.info(f"Total Parquet processing time: {sum(CSVHandler.parquet_processing_times):.2f} seconds")
    logging.info(f"Total Parquet size: {total_parquet_size_mb:.2f} MB")
    logging.info(f"Total Parquet files converted: {CSVHandler.total_parquet_files}")  # Log the total Parquet files converted
