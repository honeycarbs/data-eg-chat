import pandas as pd
from sqlalchemy import create_engine, exc
from sqlalchemy.engine.base import Engine
from typing import Optional, Dict, Any
import csv
from io import StringIO

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