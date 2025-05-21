import pandas as pd
import json

# Used to transform data in a dataframe to 
# match validations and adhere to the database schema

class Transformer:
    def __init__(self, df):
        self.df = df
    
    def get_dataframe(self):
        """Returns the internal DataFrame. Used for testing this class"""
        return self.df
    
    def transform(self):
        """
        Runs all validation checks and data transformations.
        Returns a data frame object.
        """

        # Create new columns needed for DB
        self.createSpeed()
        self.createTimestamp()
        self.renameColumns()
        self.dropColumns()
        
    def createSpeed(self):
        """
        Creates a speed column in the DataFrame by calculating:
        (current_row['METERS'] - previous_row['METERS']) / 
        (current_row['ACT_TIME'] - previous_row['ACT_TIME'])

        This is computed within each EVENT_NO_TRIP group (i.e., per trip).
        The first row of each group will have its speed set equal to the next row's speed if available.
        """

        def compute_speed(group):
            delta_meters = group['METERS'].diff()
            delta_time = group['ACT_TIME'].diff()
            speed = delta_meters / delta_time

            # Fill first row's speed with second row's speed if available
            if len(speed) > 1:
                speed.iloc[0] = speed.iloc[1]
            return speed

        self.df['speed'] = (
            self.df
            .groupby('EVENT_NO_TRIP', group_keys=False)[['METERS', 'ACT_TIME']]
            .apply(compute_speed)
        )
    
    def createTimestamp(self):
        """
        Compute timestamp (tstamp) from ODP_DATE and ACT_TIME
        """
        # Convert 'OPD_DATE' to datetime objects
        base_dates = pd.to_datetime(self.df['OPD_DATE'], format='%d%b%Y:%H:%M:%S')

        # Convert 'ACT_TIME' to timedelta (seconds)
        time_offsets = pd.to_timedelta(self.df['ACT_TIME'], unit='s')

        # Compute the final TIMESTAMP by adding the time offset to the base date
        self.df['tstamp'] = base_dates + time_offsets
    
    def renameColumns(self):
        """
        Renames columns to match database schema.
        """
        self.df.rename(columns={
            'EVENT_NO_TRIP': 'trip_id',
            'GPS_LONGITUDE': 'longitude',
            'GPS_LATITUDE': 'latitude',
            'VEHICLE_ID': 'vehicle_id'
        }, inplace=True)
    
    def dropColumns(self):
        """
        Drops columns that are not needed for the database.
        """
        columns_to_drop = [
            'EVENT_NO_STOP',
            'OPD_DATE',
            'METERS',
            'ACT_TIME',
            'GPS_SATELLITES',
            'GPS_HDOP'
        ]
        self.df.drop(columns=columns_to_drop, inplace=True, errors='ignore')

    def createTripDF(df):
            trip_df = df[['trip_id', 'vehicle_id']].copy()
            trip_df = trip_df.drop_duplicates(subset=['trip_id'])

            return trip_df

    def createBreadcrumbDF(df):
            breadcrumb_df = df[['tstamp', 'latitude', 'longitude', 'speed', 'trip_id']].copy()
            
            return breadcrumb_df

def main():
    """
        Main function used for TESTING purposes. Include a local json file
        and modify file_path accordingly to test the transformations.
    """
    file_path = 'data-2025-05-08.json'

    try:
        with open(file_path, 'r') as file:
            data = json.load(file)

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
        transformer.transform()
        print(transformer.get_dataframe())

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
    except json.JSONDecodeError:
        print("Error: Failed to decode JSON from the file.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
