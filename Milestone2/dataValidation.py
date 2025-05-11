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
    
    def validate(self):
        self.validateTstamp()
        self.validateDate()
        self.validateNoDuplicateTstampTripID()
        self.validateLatitudeRange()
        self.validateLongitudeRange()
        self.validateSpeedGreaterThanZero()
        self.validateTimestampMatchesDate()
        self.validateTripIdOneVehicle()
        self.validateSpeedDistribution()


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

    def validateDirection(self):
        """
        Ensures that the 'DIRECTION' column only contains valid values (1 or 0).
        Returns True if valid, False otherwise.
        """
        print("Running validateDirection...")

        if 'DIRECTION' not in self.df.columns:
            print("Missing 'DIRECTION' column in the dataframe!")
            return False

        invalid_direction_mask = ~self.df['DIRECTION'].isin([0, 1])

        if invalid_direction_mask.any():
            print(f"Found {invalid_direction_mask.sum()} invalid 'DIRECTION' values.")
            return False

        print("All DIRECTION values are valid (either 1 or 0).")
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

    def validateTripIDForeignKey(self, trip_df):
        """
        Validates that all trip_id values in BreadCrumb exist in the Trip table.
        Invalid trip_ids are set to NaN.
        Returns True if all trip_id values are valid or can be dropped.
        """
        print("Running validateTripIDForeignKey...")

        if 'trip_id' not in self.df.columns:
            print("Missing 'trip_id' column in the BreadCrumb dataframe!")
            return False

        if 'trip_id' not in trip_df.columns:
            print("Missing 'trip_id' column in the Trip dataframe!")
            return False

        valid_trip_ids = set(trip_df['trip_id'].unique())
        mask_invalid = ~self.df['trip_id'].isin(valid_trip_ids)

        if mask_invalid.any():
            invalid_count = mask_invalid.sum()
            print(f"Found {invalid_count} invalid trip_id values. Setting them to NaN...")
            self.df.loc[mask_invalid, 'trip_id'] = float('nan')

        if self.df['trip_id'].isna().any():
            print(f"{self.df['trip_id'].isna().sum()} trip_id values remain invalid.")
            return False

        print("All trip_id values in BreadCrumb are valid.")
        return True
    
    def validateServiceKey(self):
        """
        Validates that all service_key values are within the allowed range: ['Weekday', 'Saturday', 'Sunday'].
        Invalid values are set to NaN and interpolation is attempted.
        Returns True if 'service_key' column exists and interpolation succeeds.
        """
        print("Running validateServiceKey...")

        valid_service_keys = ['Weekday', 'Saturday', 'Sunday']

        if 'service_key' not in self.df.columns:
            print("Missing 'service_key' column in the dataframe!")
            return False

        # Mark invalid values as NaN
        mask_invalid_service_key = ~self.df['service_key'].isin(valid_service_keys)
        self.df.loc[mask_invalid_service_key, 'service_key'] = float('nan')

        if self.df['service_key'].isna().any():
            print(f"Interpolating {self.df['service_key'].isna().sum()} invalid service_key values...")
            self.df.reset_index(drop=True, inplace=True)
            self.df['service_key'] = self.df['service_key'].interpolate(method='pad', limit_direction='both')

        # Final check to ensure all values are valid after interpolation
        still_invalid_service_key = self.df['service_key'].isna() | ~self.df['service_key'].isin(valid_service_keys)
        if still_invalid_service_key.any():
            print("Some service_key values remain invalid or could not be interpolated.")
            return False

        print("All service_key values are valid and within the allowed range ['Weekday', 'Saturday', 'Sunday'].")
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

        # Instantiate your Transformer
        transformer = Transformer(df)
        transformer.transform()

        validator = Validation(transformer.get_dataframe())
        validator.validate()
        print(validator.get_dataframe())

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
    except json.JSONDecodeError:
        print("Error: Failed to decode JSON from the file.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()