import pandas as pd
import json
import dataValidation
import re

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
        self.createDirection()
        self.createServiceKey()

        validator = dataValidation.Validation(self.df)

        # Timestamp that is not a datetime object found
        if not validator.validateTstamp():
            self.interpolateInvalidTimestamps()
            validator = dataValidation.Validation(self.df)

        # Date not following regex found
        if not validator.validateDate():
            self.removeInvalidDate()
            validator = dataValidation.Validation(self.df)

        # Invalid direction value found
        # TODO: Impossible case for now
        if not validator.validateDirection():
            pass

        # Invalid latitude(s) found, remove associated rows.
        if not validator.validateLatitudeRange():
            self.removeInvalidLatitudes()
            validator = dataValidation.Validation(self.df)

        # Invalid longitude(s) found, remove associated rows.
        if not validator.validateLongitudeRange():
            self.removeInvalidLongitudes()
            validator = dataValidation.Validation(self.df)

        # Negative speed values
        if not validator.validateSpeedGreaterThanZero():
            self.removeInvalidSpeeds()
            validator = dataValidation.Validation(self.df)

        # Duplicate timestamp & trip ID rows found
        if not validator.validateNoDuplicateTstampTripID():
            self.removeDuplicateTstampTripID()
            validator = dataValidation.Validation(self.df)
        

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

    def createDirection(self):
        """
        Creates a new column 'DIRECTION' and assigns the value 0 to all rows.
        """
        self.df['DIRECTION'] = 0

    def createServiceKey(self):
        """
        Creates a new column 'SERVICE_KEY' based on the day of the week extracted from 'ODP_DATE':
        """
        if 'ODP_DATE' not in self.df.columns:
            raise ValueError("Missing 'ODP_DATE' column in the dataframe!")

        # Convert ODP_DATE to datetime (format assumed to be already validated)
        dates = pd.to_datetime(self.df['ODP_DATE'], format='%d%b%Y:%H:%M:%S', errors='coerce')

        # Map weekday number to service key
        def get_service_key(date):
            if pd.isna(date):
                return None
            weekday = date.weekday()
            if weekday < 5:
                return 'Weekday'
            elif weekday == 5:
                return 'Saturday'
            else:
                return 'Sunday'

        self.df['SERVICE_KEY'] = dates.apply(get_service_key)

    def interpolateInvalidTimestamps(self):
        """
        Identifies and replaces non-datetime entries in the TIMESTAMP column 
        by interpolating from valid datetime entries.
        """
        # Ensure TIMESTAMP column exists
        if 'TIMESTAMP' not in self.df.columns:
            raise ValueError("TIMESTAMP column not found in DataFrame.")

        # Mark invalid timestamps
        invalid_mask = ~self.df['TIMESTAMP'].apply(lambda x: isinstance(x, pd.Timestamp))
        
        if invalid_mask.any():
            print(f"Found {invalid_mask.sum()} invalid TIMESTAMP entries. Interpolating...")

            # Convert invalid entries to NaT so interpolation can be performed
            self.df.loc[invalid_mask, 'TIMESTAMP'] = pd.NaT

            # Interpolate missing timestamps
            self.df['TIMESTAMP'] = self.df['TIMESTAMP'].interpolate(method='time')

            # If interpolation at the start or end is still NaT, fill with forward/backward fill
            self.df['TIMESTAMP'].fillna(method='ffill', inplace=True)
            self.df['TIMESTAMP'].fillna(method='bfill', inplace=True)

    def removeInvalidDate(self):
        """
        Removes rows from the DataFrame where 'OPD_DATE' does not match the expected format: DDMMMYYYY:HH:MM:SS.
        Example of a valid format: '08DEC2022:00:00:00'
        """

        if 'OPD_DATE' not in self.df.columns:
            raise ValueError("Missing 'OPD_DATE' column in the dataframe!")

        # Define the regex pattern for valid OPD_DATE
        pattern = re.compile(r"^\d{2}[A-Z]{3}\d{4}:\d{2}:\d{2}:\d{2}$")

        # Create a mask of valid dates
        is_valid = self.df['OPD_DATE'].astype(str).apply(lambda x: bool(pattern.match(x)))

        # Count and remove invalid rows
        removed_count = (~is_valid).sum()
        if removed_count > 0:
            print(f"Removing {removed_count} rows with invalid 'OPD_DATE' values.")
            self.df = self.df[is_valid].reset_index(drop=True)
        else:
            print("No invalid 'OPD_DATE' values found.")

    def removeInvalidLatitudes(self):
        """
        Removes rows where the GPS_LATITUDE is not within the valid range (-90 to 90).
        """

        # Check if the 'GPS_LATITUDE' column exists
        if 'GPS_LATITUDE' not in self.df.columns:
            raise ValueError("Missing 'GPS_LATITUDE' column in the dataframe!")

        # Create a mask for valid latitudes within the range -90 to 90
        valid_latitudes = (self.df['GPS_LATITUDE'] >= -90) & (self.df['GPS_LATITUDE'] <= 90)

        # Keep only rows with valid latitudes
        self.df = self.df[valid_latitudes]

        print(f"Removed rows with invalid GPS_LATITUDE values outside the range (-90, 90).")

    def removeInvalidLongitudes(self):
        """
        Removes rows where 'GPS_LONGITUDE' is out of bounds (not within -180 to 180).
        """
        if 'GPS_LONGITUDE' not in self.df.columns:
            raise ValueError("Missing 'GPS_LONGITUDE' column in the dataframe!")

        # Check if 'GPS_LONGITUDE' values are within valid longitude bounds
        valid_longitudes = (self.df['GPS_LONGITUDE'] >= -180) & (self.df['GPS_LONGITUDE'] <= 180)

        # Remove rows where 'GPS_LONGITUDE' is out of bounds
        self.df = self.df[valid_longitudes]

    def removeInvalidSpeeds(self):
        """
        Removes rows from the DataFrame where the 'SPEED' column has negative values.
        """
        if 'SPEED' not in self.df.columns:
            raise ValueError("Missing 'SPEED' column in the dataframe!")

        # Remove rows where 'SPEED' is negative
        self.df = self.df[self.df['SPEED'] >= 0]

        print(f"Removed rows with negative values in 'SPEED'. Remaining rows: {len(self.df)}")

    def removeDuplicateTstampTripID(self):
        """
        Removes rows from the DataFrame that have duplicate pairs of 'TIMESTAMP' and 'EVENT_NO_TRIP'.
        Keeps the first occurrence of each pair and removes any subsequent duplicates.
        """
        if 'TIMESTAMP' not in self.df.columns or 'EVENT_NO_TRIP' not in self.df.columns:
            raise ValueError("Missing 'TIMESTAMP' or 'EVENT_NO_TRIP' column in the dataframe!")

        # Drop duplicate rows based on the combination of 'TIMESTAMP' and 'EVENT_NO_TRIP'
        self.df = self.df.drop_duplicates(subset=['TIMESTAMP', 'EVENT_NO_TRIP'], keep='first')

        print(f"Removed duplicate rows based on 'TIMESTAMP' and 'EVENT_NO_TRIP'. Remaining rows: {len(self.df)}")

        
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

        transformer = Transformer(df)
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
