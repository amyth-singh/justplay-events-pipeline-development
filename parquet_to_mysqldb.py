import os
import logging
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
import yaml

# Configure logging
logging.basicConfig(filename='conversion_log.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

def get_database_credentials():
    try:
        with open("config.yaml", 'r') as stream:
            credentials = yaml.safe_load(stream)
            return credentials['database']
    except FileNotFoundError:
        logging.error("Config file not found!")
        return None

def get_schema_from_file():
    try:
        with open("schema_sql.yaml", 'r') as stream:
            schema = yaml.safe_load(stream)
            return schema
    except FileNotFoundError:
        logging.error("Schema file not found!")
        return None

def table_exists(cursor, table_name):
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    return bool(cursor.fetchone())

def create_table():
    conn = None
    db_credentials = get_database_credentials()
    schema = get_schema_from_file()
    
    if db_credentials and schema:
        try:
            conn = mysql.connector.connect(**db_credentials)
            cursor = conn.cursor()
            table_name = next(iter(schema))
            
            if not table_exists(cursor, table_name):
                columns = schema[table_name]
                create_table_query = f"CREATE TABLE {table_name} ("
                
                for column in columns:
                    column_name = column['name']
                    column_type = column['type']
                    create_table_query += f"{column_name} {column_type}, "
                    
                create_table_query = create_table_query[:-2]
                create_table_query += ")"
                
                cursor.execute(create_table_query)
                logging.info(f"'{table_name}' table created successfully in schema '{db_credentials['database']}'")
            else:
                logging.info("Table already exists")
                
        except mysql.connector.Error as e:
            logging.error(f"Error creating table: {e}")
            
        finally:
            if conn and conn.is_connected():
                cursor.close()
                conn.close()
    else:
        logging.error("Database credentials or schema not available")

def parquet_to_dataframe(folder_path):
    try:
        dfs = []  # List to store DataFrames for each file
        for file_name in os.listdir(folder_path):
            if file_name.endswith('.parquet'):
                file_path = os.path.join(folder_path, file_name)
                df = pd.read_parquet(file_path)
                if not df.empty:
                    dfs.append(df)
                else:
                    logging.warning(f"The Parquet file {file_path} is empty.")
        if dfs:
            concatenated_df = pd.concat(dfs, ignore_index=True)
            unique_df = concatenated_df.drop_duplicates().copy()  # Make a copy to avoid SettingWithCopyWarning
            
            # Add new columns using .loc to avoid SettingWithCopyWarning
            unique_df.loc[:, 'creation_timestamp'] = datetime.now()
            unique_df.loc[:, 'id'] = range(len(unique_df))
            
            return unique_df
        else:
            logging.warning("No non-empty Parquet files found in the folder.")
            return None
    except Exception as e:
        logging.error(f"An error occurred while reading Parquet files: {e}")
        return None

def load_df_to_mysql(df, db_config, table_name):
    try:
        connection_string = f"mysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        engine = create_engine(connection_string)
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        logging.info(f"DataFrame successfully loaded into MySQL table '{table_name}'")
    except Exception as e:
        logging.error(f"An error occurred while loading DataFrame into MySQL: {e}")


if __name__ == "__main__":
    folder_path = "C:\\Users\\amith\\Desktop\\code_elt\\justplay-infra-pipeline-development\\output_parquet"
    df = parquet_to_dataframe(folder_path)
    if df is not None:
        table_name = "student_data"
        db_config = get_database_credentials()
        create_table()
        load_df_to_mysql(df, db_config, table_name)

