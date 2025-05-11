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

        # Instantiate your Transformer
        transformer = Transformer(df)
        transformer.createSpeed()
        transformer.createTimestamp()

        # print(transformer.get_dataframe().head())
        dataframe_updated = transformer.get_dataframe()
        print(dataframe_updated.head())


        """
        Database testing. Temoprarily renaming columns by hand.
        """
        db_uri = "postgresql://postgres:postgres@localhost:5432/testdb"

        breadcrumb_df = df[['TIMESTAMP', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'SPEED', 'EVENT_NO_TRIP']].copy()
    
        breadcrumb_df = breadcrumb_df.rename(columns={
            'TIMESTAMP': 'tstamp',
            'GPS_LATITUDE': 'latitude',
            'GPS_LONGITUDE': 'longitude',
            'SPEED': 'speed',
            'EVENT_NO_TRIP': 'trip_id'
        })

        trip_df = df[['EVENT_NO_TRIP']].copy()
    
        # Rename column and add empty columns as per schema
        trip_df = trip_df.rename(columns={'EVENT_NO_TRIP': 'trip_id'})
        trip_df = trip_df.drop_duplicates()  # Only need one record per trip
        
        # Add empty columns as specified
        trip_df['route_id'] = None
        trip_df['vehicle_id'] = None
        trip_df['service_key'] = None
        trip_df['direction'] = None
        
        with DataFrameSQLInserter(db_uri) as inserter:
            inserter.insert_dataframe(trip_df, "trip")
            inserter.insert_dataframe(breadcrumb_df, "breadcrumb")


    except Exception as e:
        print(e)