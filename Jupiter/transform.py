import pandas as pd
import json

# Used to transform data in a dataframe to 
# match validations and adhere to the database schema

class Transform:
    def __init__(self, df):
        """
        Ensure Transform class is initialized with a pandas DataFrame.
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Expected df to be a pandas DataFrame.")
        self.df = df
    
    def get_dataframe(self):
        """Returns the internal DataFrame. Used for testing this class"""
        return self.df
    
    def start(self):
        """
        Runs all data transformations.
        Returns a data frame object.
        """
        self.createSpeed()
        self.createTimestamp()
        return self.df
    
    def createSpeed(self):
        """
        Creates a SPEED column in the DataFrame by calculating:
        (current_row['METERS'] - previous_row['METERS']) / 
        (current_row['ACT_TIME'] - previous_row['ACT_TIME'])

        Note: 1st row is handled by copying 2nd row's value.
        """

        # Calculate the difference
        delta_meters = self.df['METERS'].diff()
        delta_time = self.df['ACT_TIME'].diff()

        # Compute speed (will be NaN for the first row)
        self.df['SPEED'] = delta_meters / delta_time

        # Set Row 0's SPEED equal to Row 1's SPEED
        if len(self.df) > 1:
            self.df.at[0, 'SPEED'] = self.df.at[1, 'SPEED']
    
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
    To be used manually.
    Tests a local JSON file for correct transformations.
    Update file_path accordingly.
    """
    # Path to your JSON file
    file_path = '2907.json'

    # Read the JSON file into a pandas dataframe
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)

        # Convert the JSON data to a pandas dataframe
        df = pd.DataFrame(data)

        transformer = Transform(df)
        transformer.createSpeed()
        transformer.createTimestamp()

        # Output the resulting dataframe
        print(transformer.get_dataframe())

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
    except json.JSONDecodeError:
        print("Error: Failed to decode JSON from the file.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
