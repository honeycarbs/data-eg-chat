import pandas as pd
from sqlalchemy import create_engine, exc
from sqlalchemy.engine.base import Engine
from typing import Optional, Dict, Any
import csv
from io import StringIO

from transformer import Transformer
from dataValidation import Validation
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
    
    def insert_dataframe(self, df: pd.DataFrame, table_name: str, if_exists: str = "append", index: bool = False, dtype: Optional[Dict[str, Any]] = None, method: Optional[str] = None) -> int:
        if self.engine is None:
            self.connect()
        
        try:
            rows_inserted = df.to_sql(
                table_name,
                con=self.engine,
                if_exists=if_exists,
                index=index,
                dtype=dtype,
                method=method,
                chunksize=self.batch_size,
            )
            print(f"Successfully inserted {rows_inserted} rows into '{table_name}'.")
        except exc.SQLAlchemyError as e:
            raise RuntimeError(f"Failed to insert data: {e}") from e
        
        return rows_inserted
    
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

        # Instantiate the Validator
        validator = Validation(df)
        validator.validateBeforeTransform()

        validated_df = validator.get_dataframe()

        transformer = Transformer(validated_df)
        transformer.transform()
        transformed_df = transformer.get_dataframe()
        print(transformed_df.head())


        """
        This is how the function is intended to be used. This worked on my local PostgreSQL, sh should work on vm.
        """
        db_uri = "postgresql://postgres:postgres@localhost:5432/testdb"

        """
        Important: dataframe has to be prepared for insertion for both tables. For now,
        I will do it here manually. Later, it can be added into transformer. Gere is what needs to be done:
        """
        def prepare_for_trip_table(df):
            trip_df = df[['trip_id', 'vehicle_id']].copy()

            trip_df = trip_df.drop_duplicates(subset=['trip_id'])
            return trip_df
        
        def prepare_for_breadcrumb_table(df):
            breadcrumb_df = df[['tstamp', 'latitude', 'longitude', 'speed', 'trip_id']].copy()

            return breadcrumb_df
        
        dataframe_trip = prepare_for_trip_table(transformed_df)
        dataframe_breadcrumb = prepare_for_breadcrumb_table(transformed_df)

        with DataFrameSQLInserter(db_uri) as inserter:
            inserter.insert_dataframe(dataframe_trip, "trip")
            inserter.insert_dataframe(dataframe_breadcrumb, "breadcrumb")


    except Exception as e:
        print(e)