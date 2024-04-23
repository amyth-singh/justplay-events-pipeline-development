import os
import time
import pandas as pd
import logging
import yaml
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime

class CSVHandler(FileSystemEventHandler):
    """
    Handles CSV file events.
    """
    def __init__(self, input_folder, output_folder, schema_file):
        """
        Initializes CSVHandler.
        """
        super().__init__()
        self.counter = 0
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.schema_file = schema_file
        self.schema = self.load_schema(schema_file)

    def load_schema(self, schema_file):
        """
        Loads schema from a YAML file.
        """
        with open(schema_file, 'r') as f:
            schema = yaml.safe_load(f)
        return schema

    def validate_schema(self, df):
        """
        Validates schema against DataFrame columns.
        """
        csv_columns = set(df.columns)
        schema_columns = set(self.schema.keys())
        return csv_columns == schema_columns

    def on_created(self, event):
        """
        Called when a new file is created in the watched directory.
        """
        if event.is_directory or not event.src_path.endswith('.csv'):
            return
        
        try:
            df = pd.read_csv(event.src_path, delimiter=';')
            df.dropna(inplace=True)
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
            self.move_to_failed(event.src_path)  # Move the unsuccessful CSV file to 'output_failed' folder
            return
        
        parquet_filename = os.path.join(self.output_folder, f"{os.path.splitext(os.path.basename(event.src_path))[0]}.parquet")
        df.to_parquet(parquet_filename)
        
        logging.info(f"Converted {event.src_path} to Parquet format")
        print(f"Conversion successful: {event.src_path} uploaded to {parquet_filename}")
        
        self.delete_csv(event.src_path)  # Delete the CSV file from the input folder after successful conversion

    def move_to_failed(self, csv_file):
        """
        Move the unsuccessful CSV file to 'output_failed' folder.
        """
        failed_folder = os.path.join(os.path.dirname(csv_file), 'output_failed')
        if not os.path.exists(failed_folder):
            os.makedirs(failed_folder)
        filename = os.path.basename(csv_file)
        dest = os.path.join(failed_folder, filename)
        os.rename(csv_file, dest)
        logging.info(f"Moved unsuccessful CSV file {csv_file} to output_failed folder")

    def delete_csv(self, csv_file):
        """
        Delete the CSV file from the input folder.
        """
        os.remove(csv_file)
        logging.info(f"Deleted CSV file {csv_file}")

def watch_input_csv_folder(input_folder, output_folder, schema_file):
    """
    Monitors the input CSV folder for new files and converts them to Parquet format.
    """
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
    
    if not os.path.exists(output_parquet_folder):
        os.makedirs(output_parquet_folder)
    
    log_file = 'conversion_log.txt'
    logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    watch_input_csv_folder(input_csv_folder, output_parquet_folder, schema_file)
