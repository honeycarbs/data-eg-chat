import pandas as pd
import json

# Used to transform data in a dataframe to 
# match validations and adhere to the database schema

class Transformer:
    def __init__(self, df):
        """
        Ensure Transform class is initialized with a pandas DataFrame.
        """
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
        
    def createSpeed(self):
        """
        Creates a SPEED column in the DataFrame by calculating:
        (current_row['METERS'] - previous_row['METERS']) / 
        (current_row['ACT_TIME'] - previous_row['ACT_TIME'])

        This is computed within each EVENT_NO_TRIP group (i.e., per trip).
        The first row of each group will have its SPEED set equal to the next row's SPEED if available.
        """

        def compute_speed(group):
            delta_meters = group['METERS'].diff()
            delta_time = group['ACT_TIME'].diff()
            speed = delta_meters / delta_time

            # Fill first row's speed with second row's speed if it exists
            if len(speed) > 1:
                speed.iloc[0] = speed.iloc[1]
            return speed

        self.df['SPEED'] = self.df.groupby('EVENT_NO_TRIP', group_keys=False).apply(compute_speed)
    
    def createTimestamp(self):
        """
        Compute timestamp from ODP_DATE and ACT_TIME
        """
        # Convert 'OPD_DATE' to datetime objects
        base_dates = pd.to_datetime(self.df['OPD_DATE'], format='%d%b%Y:%H:%M:%S')

        # Convert 'ACT_TIME' to timedelta (seconds)
        time_offsets = pd.to_timedelta(self.df['ACT_TIME'], unit='s')

        # Compute the final TIMESTAMP by adding the time offset to the base date
        self.df['TIMESTAMP'] = base_dates + time_offsets

        
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
        transformer.createSpeed()
        transformer.createTimestamp()

        print(transformer.get_dataframe())

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
    except json.JSONDecodeError:
        print("Error: Failed to decode JSON from the file.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
