import pandas as pd
from datetime import datetime
import re

class Validation:
    def __init__(self, df):
        self.df = df
    
    def get_dataframe(self):
        """
        Returns the internal dataframe used for testing.
        """
        return self.df
    
    def validate(self):
        self.validateTstamp(self)
        self.validateDate(self)
        self.validateNoDuplicateTstampTripID(self)
        self.validateLatitudeRange(self)
        self.validateLongitudeRange(self)
        self.validateSpeedGreaterThanZero(self)


    def validateTstamp(self):
        """
        Validates and interpolates the 'TIMESTAMP' column in the DataFrame.
        Converts non-datetime entries to NaT, then uses pandas interpolate to fill missing values.
        Returns True if 'TIMESTAMP' exists and interpolation succeeds.
        """
        print("Running validateTstamp...")

        if 'TIMESTAMP' not in self.df.columns:
            print("Missing 'TIMESTAMP' column in the dataframe!")
            return False

        # Coerce all non-datetime entries to NaT
        self.df['TIMESTAMP'] = pd.to_datetime(self.df['TIMESTAMP'], errors='coerce')

        # Check if any NaT values remain
        if self.df['TIMESTAMP'].isna().any():
            # Set a numeric index if necessary
            self.df.reset_index(drop=True, inplace=True)
            self.df['TIMESTAMP'] = self.df['TIMESTAMP'].interpolate(method='time', limit_direction='both')

        # Final check
        if self.df['TIMESTAMP'].isna().any():
            print("Interpolation failed for some entries.")
            return False

        print("All 'TIMESTAMP' values are now valid datetime objects.")
        return True

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

    def validateNoDuplicateTstampTripID(self):
        """
        Removes duplicate (TIMESTAMP, EVENT_NO_TRIP) pairs from the DataFrame.
        Returns True if 'TIMESTAMP' and 'EVENT_NO_TRIP' columns exist, False otherwise.
        """
        print("Running validateNoDuplicateTstampTripID...")

        if 'TIMESTAMP' not in self.df.columns or 'EVENT_NO_TRIP' not in self.df.columns:
            print("Missing 'TIMESTAMP' or 'EVENT_NO_TRIP' columns!")
            return False

        self.df.drop_duplicates(subset=['TIMESTAMP', 'EVENT_NO_TRIP'], inplace=True)
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

    def validateSpeedGreaterThanZero(self):
        """
        Validates that all SPEED values are greater than 0.
        Negative or zero SPEED values are set to NaN and interpolated.
        Returns True if 'SPEED' column exists and interpolation succeeds.
        """
        print("Running validateSpeedGreaterThanZero...")

        if 'SPEED' not in self.df.columns:
            print("Missing 'SPEED' column in the dataframe!")
            return False

        # Replace non-positive SPEED values with NaN
        invalid_speed_mask = self.df['SPEED'] <= 0
        self.df.loc[invalid_speed_mask, 'SPEED'] = float('nan')

        if self.df['SPEED'].isna().any():
            print(f"Interpolating {self.df['SPEED'].isna().sum()} non-positive SPEED values...")
            self.df.reset_index(drop=True, inplace=True)
            self.df['SPEED'] = self.df['SPEED'].interpolate(method='linear', limit_direction='both')

        # Final validation check
        if (self.df['SPEED'] <= 0).any() or self.df['SPEED'].isna().any():
            print("Some SPEED values remain non-positive or could not be interpolated.")
            return False

        print("All SPEED values are greater than 0 after interpolation.")
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

    def validateSpeedDistribution(self):
        """
        Checks if the distribution of SPEED values looks reasonable.
        Flags speeds that are more than 3 standard deviations away from the mean.
        Returns True if valid, False otherwise.
        """
        print("Running validateSpeedDistribution...")

        mean_speed = self.df['SPEED'].mean()
        std_speed = self.df['SPEED'].std()

        outlier_mask = (self.df['SPEED'] < (mean_speed - 3 * std_speed)) | (self.df['SPEED'] > (mean_speed + 3 * std_speed))

        if outlier_mask.any():
            print(f"Found {outlier_mask.sum()} outlier SPEED values.")
            return False

        print("Speed values have a reasonable distribution.")
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