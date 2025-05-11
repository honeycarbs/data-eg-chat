import pandas as pd
from sqlalchemy import create_engine, exc
from sqlalchemy.engine.base import Engine
from typing import Optional, Dict, Any
import csv
from io import StringIO

from transformer import Transformer
from json import load, JSONDecodeError

def psql_insert_copy(table, conn, keys, data_iter):
    """
    Custom method for PostgreSQL COPY (fastest bulk insert).
    """
    cursor = conn.connection.cursor()
    buf = StringIO()

    writer = csv.writer(buf)
    writer.writerows(data_iter)
    buf.seek(0)

    cursor.copy_from(buf, table, sep=',', columns=keys)
    conn.connection.commit()


class DataFrameSQLInserter:
    """
    A class to handle insertion of pandas DataFrames into SQL database
    """
    def __init__(self, db_uri: str, batch_size: int = 1000, **engine_kwargs):
        self.db_uri = db_uri
        self.batch_size = batch_size
        self.engine_kwargs = engine_kwargs
        self.engine: Optional[Engine] = None
        
    def connect(self):
        try:
            self.engine = create_engine(self.db_uri, **self.engine_kwargs)
            print(f"Connected to database: {self.db_uri}")
        except exc.SQLAlchemyError as e:
            raise ConnectionError(f"Failed to connect to database: {e}") from e
    
    def disconnect(self):
        if self.engine is not None:
            self.engine.dispose()
            print("Database connection closed.")
    
    def insert_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = "fail", index: bool = False, dtype: Optional[Dict[str, Any]] = None, method: Optional[str] = None):
        if self.engine is None:
            self.connect()
        
        try:
            df.to_sql(
                table_name,
                con=self.engine,
                if_exists=if_exists,
                index=index,
                dtype=dtype,
                method=method,
                chunksize=self.batch_size,
            )
            print(f"Successfully inserted {len(df)} rows into '{table_name}'.")
        except exc.SQLAlchemyError as e:
            raise RuntimeError(f"Failed to insert data: {e}") from e
    
    def __enter__(self):
        """Context manager entry (for 'with' statement)."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit (ensures connection is closed)."""
        self.disconnect()



if __name__ == "__main__":
    """
    From Cameron's file
    """
    file_path = 'data-2025-05-08.json'

    try:
        with open(file_path, 'r') as file:
            data = load(file)

        # Step 1: Convert list of strings to list of dictionaries
        raw_messages = data.get('messages', [])
        parsed_messages = []
        for msg in raw_messages:
            fields = dict(field.strip().split(': ', 1) for field in msg.split(', '))
            parsed_messages.append(fields)

        # Step 2: Convert to DataFrame
        df = pd.DataFrame(parsed_messages)

        # Optional: Convert numeric columns
        numeric_cols = ['EVENT_NO_TRIP', 'EVENT_NO_STOP', 'VEHICLE_ID', 'METERS', 'ACT_TIME',
                        'GPS_LONGITUDE', 'GPS_LATITUDE', 'GPS_SATELLITES', 'GPS_HDOP']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Instantiate your Transformer
        transformer = Transformer(df)
        transformer.createSpeed()
        transformer.createTimestamp()

        df_updated = transformer.get_dataframe()
        print(df_updated)

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
    except JSONDecodeError:
        print("Error: Failed to decode JSON from the file.")
    except Exception as e:
        print(f"An error occurred: {e}")


    
    """
    Database testing
    """
    db_uri = "postgresql://postgres:postgres@localhost:5432/testdb"
    
    with DataFrameSQLInserter(db_uri) as inserter:
        inserter.insert_dataframe(df_updated, "Trip")