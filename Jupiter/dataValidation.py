import pandas as pd
from datetime import datetime
import re
import json
from transformer import Transformer

class Validation:
    def __init__(self, df):
        self.df = df
    
    def get_dataframe(self):
        """
        Returns the internal dataframe used for testing.
        """
        return self.df
    
    def validateBeforeTransform(self):
        self.removeInvalidLatitude()
        self.removeInvalidLongitude()
        self.validateDate()
        self.validateLatitudeRange()
        self.validateLongitudeRange()
        self.validateTripIdOneVehicle()
        self.validateEventNoTrip()
        self.validateEventNoStop()
        self.validateMeters()
    
    def validateAfterTransform(self):
        self.validateSpeed()

    def validateDate(self):
        """
        Ensures 'OPD_DATE' values match the format 'DDMMMYYYY:HH:MM:SS' (e.g., '08DEC2022:00:00:00').
        Invalid entries are coerced to NaT and interpolated.
        Returns True if all values are valid or successfully interpolated.
        """
        print("Running validateDate...")

        if 'OPD_DATE' not in self.df.columns:
            print("Missing 'OPD_DATE' column in the dataframe!")
            return False

        # Define the regex pattern and function to apply
        pattern = re.compile(r"^\d{2}[A-Z]{3}\d{4}:\d{2}:\d{2}:\d{2}$")

        # Convert valid matches to datetime, others to NaT
        def parse_opd_date(val):
            if isinstance(val, str) and pattern.match(val):
                try:
                    return datetime.strptime(val, "%d%b%Y:%H:%M:%S")
                except ValueError:
                    return pd.NaT
            return pd.NaT

        self.df['OPD_DATE'] = self.df['OPD_DATE'].apply(parse_opd_date)

        # Interpolate missing (NaT) values
        if self.df['OPD_DATE'].isna().any():
            print(f"Interpolating {self.df['OPD_DATE'].isna().sum()} invalid 'OPD_DATE' entries...")
            self.df.reset_index(drop=True, inplace=True)
            self.df['OPD_DATE'] = self.df['OPD_DATE'].interpolate(method='time', limit_direction='both')

        # Final check
        if self.df['OPD_DATE'].isna().any():
            print("Interpolation failed for some 'OPD_DATE' entries.")
            return False

        # Reformat interpolated datetime values back to original string format
        self.df['OPD_DATE'] = self.df['OPD_DATE'].dt.strftime("%d%b%Y:%H:%M:%S").str.upper()

        print("All 'OPD_DATE' values are now valid and formatted correctly.")
        return True

    def validateLatitudeRange(self):
        """
        Validates that all GPS_LATITUDE values are within the Portland bus range [45, 46].
        Out-of-range values are set to NaN and interpolated.
        Returns True if 'GPS_LATITUDE' column exists and interpolation succeeds.
        """
        print("Running validateLatitudeRange...")

        if 'GPS_LATITUDE' not in self.df.columns:
            print("Missing 'GPS_LATITUDE' column in the dataframe!")
            return False

        # Mark values out of the acceptable range as NaN
        mask_out_of_range = (self.df['GPS_LATITUDE'] < 45) | (self.df['GPS_LATITUDE'] > 46)
        self.df.loc[mask_out_of_range, 'GPS_LATITUDE'] = float('nan')

        if self.df['GPS_LATITUDE'].isna().any():
            print(f"Interpolating {self.df['GPS_LATITUDE'].isna().sum()} out-of-range GPS_LATITUDE values...")
            self.df.reset_index(drop=True, inplace=True)
            self.df['GPS_LATITUDE'] = self.df['GPS_LATITUDE'].interpolate(method='linear', limit_direction='both')

        # Final check to ensure all values are within range after interpolation
        still_out_of_range = (self.df['GPS_LATITUDE'] < 45) | (self.df['GPS_LATITUDE'] > 46) | self.df['GPS_LATITUDE'].isna()
        if still_out_of_range.any():
            print("Some GPS_LATITUDE values remain out of range or could not be interpolated.")
            return False

        print("All GPS_LATITUDE values are now within the Portland bus range (45 to 46).")
        return True

    def validateLongitudeRange(self):
        """
        Validates that all GPS_LONGITUDE values are within the Portland bus range [-124, -122].
        Out-of-range values are set to NaN and interpolated.
        Returns True if 'GPS_LONGITUDE' column exists and interpolation succeeds.
        """
        print("Running validateLongitudeRange...")

        if 'GPS_LONGITUDE' not in self.df.columns:
            print("Missing 'GPS_LONGITUDE' column in the dataframe!")
            return False

        # Mark out-of-range values as NaN
        mask_out_of_range = (self.df['GPS_LONGITUDE'] < -124) | (self.df['GPS_LONGITUDE'] > -122)
        self.df.loc[mask_out_of_range, 'GPS_LONGITUDE'] = float('nan')

        if self.df['GPS_LONGITUDE'].isna().any():
            print(f"Interpolating {self.df['GPS_LONGITUDE'].isna().sum()} out-of-range GPS_LONGITUDE values...")
            self.df.reset_index(drop=True, inplace=True)
            self.df['GPS_LONGITUDE'] = self.df['GPS_LONGITUDE'].interpolate(method='linear', limit_direction='both')

        # Final check
        still_out_of_range = (
            (self.df['GPS_LONGITUDE'] < -124) | 
            (self.df['GPS_LONGITUDE'] > -122) | 
            self.df['GPS_LONGITUDE'].isna()
        )
        if still_out_of_range.any():
            print("Some GPS_LONGITUDE values remain out of range or could not be interpolated.")
            return False

        print("All GPS_LONGITUDE values are now within the Portland bus range (-124 to -122).")
        return True


    def validateSummaryStats(self):
        """
        Performs summary statistics checks on GPS_LATITUDE, GPS_LONGITUDE, and SPEED.
        Returns True if valid, False otherwise.
        """
        print("Running validateSummaryStats...")

        summary_stats = self.df[['GPS_LATITUDE', 'GPS_LONGITUDE', 'SPEED']].describe()

        # Check GPS_LATITUDE range (Portland: 45 to 46)
        if not (45 <= summary_stats.loc['min', 'GPS_LATITUDE'] <= 46 and 
                45 <= summary_stats.loc['max', 'GPS_LATITUDE'] <= 46):
            print("Latitude values are outside the valid Portland range (45 to 46).")
            return False

        # Check GPS_LONGITUDE range (Portland: -124 to -122)
        if not (-124 <= summary_stats.loc['min', 'GPS_LONGITUDE'] <= -122 and 
                -124 <= summary_stats.loc['max', 'GPS_LONGITUDE'] <= -122):
            print("Longitude values are outside the valid Portland range (-124 to -122).")
            return False

        # Check SPEED > 0
        if not (summary_stats.loc['min', 'SPEED'] > 0):
            print("Speed values should be greater than 0.")
            return False

        print("Summary statistics are within expected Portland-specific ranges.")
        return True


    def validateTripIdOneVehicle(self):
        """
        Validates that each EVENT_NO_TRIP is associated with only one VEHICLE_ID.
        Returns True if valid, False otherwise.
        """
        print("Running validateTripIdOneVehicle...")

        if 'EVENT_NO_TRIP' not in self.df.columns or 'VEHICLE_ID' not in self.df.columns:
            print("Missing 'EVENT_NO_TRIP' or 'VEHICLE_ID' columns!")
            return False

        trip_vehicle_counts = self.df.groupby('EVENT_NO_TRIP')['VEHICLE_ID'].nunique()
        multiple_vehicles = trip_vehicle_counts[trip_vehicle_counts > 1]

        if not multiple_vehicles.empty:
            print(f"Found {len(multiple_vehicles)} EVENT_NO_TRIP values associated with multiple VEHICLE_IDs.")
            return False

        print("Each EVENT_NO_TRIP is associated with only one VEHICLE_ID.")
        return True

    def validateEventNoTrip(self):
        """
        Validates that all EVENT_NO_TRIP values are non-null and non-negative integers.
        Invalid values are set to NaN and interpolated.
        Returns True if 'EVENT_NO_TRIP' column exists and values are valid.
        """
        print("Running validateEventNoTrip...")

        if 'EVENT_NO_TRIP' not in self.df.columns:
            print("Missing 'EVENT_NO_TRIP' column in the dataframe!")
            return False

        # Mark invalid values (non-integer or negative) as NaN
        mask_invalid = ~self.df['EVENT_NO_TRIP'].apply(lambda x: isinstance(x, int) and x >= 0)
        self.df.loc[mask_invalid, 'EVENT_NO_TRIP'] = float('nan')

        if self.df['EVENT_NO_TRIP'].isna().any():
            print(f"Interpolating {self.df['EVENT_NO_TRIP'].isna().sum()} invalid EVENT_NO_TRIP values...")
            self.df['EVENT_NO_TRIP'] = self.df['EVENT_NO_TRIP'].interpolate(method='linear', limit_direction='both')

        # Final check to ensure all values are valid after interpolation
        still_invalid = self.df['EVENT_NO_TRIP'].isna()
        if still_invalid.any():
            print("Some EVENT_NO_TRIP values remain invalid or could not be interpolated.")
            return False

        print("All EVENT_NO_TRIP values are valid.")
        return True

    def validateEventNoStop(self):
        """
        Validates that all EVENT_NO_STOP values are non-null and non-negative integers.
        Invalid values are set to NaN and interpolated.
        Returns True if 'EVENT_NO_STOP' column exists and values are valid.
        """
        print("Running validateEventNoStop...")

        if 'EVENT_NO_STOP' not in self.df.columns:
            print("Missing 'EVENT_NO_STOP' column in the dataframe!")
            return False

        # Mark invalid values (non-integer or negative) as NaN
        mask_invalid = ~self.df['EVENT_NO_STOP'].apply(lambda x: isinstance(x, int) and x >= 0)
        self.df.loc[mask_invalid, 'EVENT_NO_STOP'] = float('nan')

        if self.df['EVENT_NO_STOP'].isna().any():
            print(f"Interpolating {self.df['EVENT_NO_STOP'].isna().sum()} invalid EVENT_NO_STOP values...")
            self.df['EVENT_NO_STOP'] = self.df['EVENT_NO_STOP'].interpolate(method='linear', limit_direction='both')

        # Final check to ensure all values are valid after interpolation
        still_invalid = self.df['EVENT_NO_STOP'].isna()
        if still_invalid.any():
            print("Some EVENT_NO_STOP values remain invalid or could not be interpolated.")
            return False

        print("All EVENT_NO_STOP values are valid.")
        return True

    def validateMeters(self):
        """
        Validates that all METERS values are non-negative.
        Invalid values are set to NaN and interpolated.
        Returns True if 'METERS' column exists and values are valid.
        """
        print("Running validateMeters...")

        if 'METERS' not in self.df.columns:
            print("Missing 'METERS' column in the dataframe!")
            return False

        # Mark negative values as NaN
        mask_negative = self.df['METERS'] < 0
        self.df.loc[mask_negative, 'METERS'] = float('nan')

        if self.df['METERS'].isna().any():
            print(f"Interpolating {self.df['METERS'].isna().sum()} invalid METERS values...")
            self.df['METERS'] = self.df['METERS'].interpolate(method='linear', limit_direction='both')

        # Final check to ensure all values are valid after interpolation
        still_negative = self.df['METERS'].isna()
        if still_negative.any():
            print("Some METERS values remain invalid or could not be interpolated.")
            return False

        print("All METERS values are valid.")
        return True
    
    def validateActTime(self):
        """
        Validates that all ACT_TIME values are valid timestamps.
        Invalid values are set to NaN and interpolated.
        Returns True if 'ACT_TIME' column exists and interpolation succeeds.
        """
        print("Running validateActTime...")

        if 'ACT_TIME' not in self.df.columns:
            print("Missing 'ACT_TIME' column in the dataframe!")
            return False

        # Ensure that ACT_TIME is a valid timestamp
        self.df['ACT_TIME'] = pd.to_datetime(self.df['ACT_TIME'], errors='coerce')

        if self.df['ACT_TIME'].isna().any():
            print(f"Interpolating {self.df['ACT_TIME'].isna().sum()} invalid ACT_TIME values...")
            self.df['ACT_TIME'] = self.df['ACT_TIME'].interpolate(method='linear', limit_direction='both')

        # Final check to ensure all values are valid after interpolation
        still_invalid = self.df['ACT_TIME'].isna()
        if still_invalid.any():
            print("Some ACT_TIME values remain invalid or could not be interpolated.")
            return False

        print("All ACT_TIME values are valid.")
        return True

    def removeInvalidLatitude(self):
        """
        Removes rows where GPS_LATITUDE is None.
        Returns True if 'GPS_LATITUDE' column exists.
        """
        print("Running removeInvalidLatitude...")

        if 'GPS_LATITUDE' not in self.df.columns:
            print("Missing 'GPS_LATITUDE' column in the dataframe!")
            return False

        initial_count = len(self.df)
        self.df = self.df[self.df['GPS_LATITUDE'].notna()]
        removed_count = initial_count - len(self.df)

        print(f"Removed {removed_count} rows with None GPS_LATITUDE.")
        return True

    def removeInvalidLongitude(self):
        """
        Removes rows where GPS_LONGITUDE is None.
        Returns True if 'GPS_LONGITUDE' column exists.
        """
        print("Running removeInvalidLongitude...")

        if 'GPS_LONGITUDE' not in self.df.columns:
            print("Missing 'GPS_LONGITUDE' column in the dataframe!")
            return False

        initial_count = len(self.df)
        self.df = self.df[self.df['GPS_LONGITUDE'].notna()]
        removed_count = initial_count - len(self.df)

        print(f"Removed {removed_count} rows with None GPS_LONGITUDE.")
        return True
    

    def validateSpeed(self):
        """
        Validates that all speed values are less than or equal to 32.0.
        Values above 32.0 are set to NaN before interpolation.
        Returns True if 'speed' column exists and interpolation succeeds.
        """
        print("Running validateSpeed...")

        if 'speed' not in self.df.columns:
            print("Missing 'speed' column in the dataframe!")
            return False

        # Mark speeds above 32.0 as NaN
        mask_too_fast = self.df['speed'] > 32.0
        self.df.loc[mask_too_fast, 'speed'] = float('nan')

        if self.df['speed'].isna().any():
            print(f"Interpolating {self.df['speed'].isna().sum()} speed values greater than 32.0...")
            self.df['speed'] = self.df['speed'].interpolate(method='linear', limit_direction='both')

        # Final check to ensure all speed values are now <= 32.0 and not NaN
        invalid_mask = self.df['speed'].isna() | (self.df['speed'] > 32.0)
        if invalid_mask.any():
            print("Some speed values remain invalid or could not be interpolated.")
            return False

        print("All speed values are now valid (≤ 32.0).")
        return True
    
    
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

        validator = Validation(df)
        validator.validateBeforeTransform()
        transformer = Transformer(validator.get_dataframe())
        transformer.transform()
        transformed_df = transformer.get_dataframe()

        Validation(transformed_df).validateAfterTransform()
        print(transformed_df)
        max_speed = transformed_df['speed'].max()
        print(f'Max speed found: {max_speed}')

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
    except json.JSONDecodeError:
        print("Error: Failed to decode JSON from the file.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()