import pandas as pd
from datetime import datetime
import re

class Validation:
    def __init__(self, df):
        # Ensure Validation class is initialized with a pandas DataFrame
        if not isinstance(df, pd.DataFrame):
            raise TypeError("Expected df to be a pandas DataFrame!")
        self.df = df
    
    def get_dataframe(self):
        """
        Returns the internal dataframe used for testing.
        """
        return self.df

    def validateTstamp(self):
        """
        Validates that 'TIMESTAMP' column exists and all values are datetime.datetime objects.
        Returns True if valid, False otherwise.
        """
        print("Running validateTstamp...")

        if 'TIMESTAMP' not in self.df.columns:
            print("Missing 'TIMESTAMP' column in the dataframe!")
            return False

        # Mask for non-datetime entries
        invalid_mask = ~self.df['TIMESTAMP'].apply(lambda x: isinstance(x, datetime))
        if invalid_mask.any():
            print(f"Found {invalid_mask.sum()} invalid 'TIMESTAMP' entries.")
            return False

        print("All 'TIMESTAMP' values are valid datetime objects.")
        return True

    def validateDate(self):
        """
        Validates that all 'OPD_DATE' values in the DataFrame match the expected format: DDMMMYYYY:HH:MM:SS.
        Example: '08DEC2022:00:00:00'
        Returns True if valid, False otherwise.
        """
        print("Running validateDate...")

        if 'OPD_DATE' not in self.df.columns:
            print("Missing 'OPD_DATE' column in the dataframe!")
            return False

        # Regular expression pattern for the expected date format
        pattern = re.compile(r"^\d{2}[A-Z]{3}\d{4}:\d{2}:\d{2}:\d{2}$")

        invalid_mask = ~self.df['OPD_DATE'].astype(str).apply(lambda x: bool(pattern.match(x)))

        if invalid_mask.any():
            print(f"Found {invalid_mask.sum()} 'OPD_DATE' values that do not match the expected format.")
            return False

        print("All 'OPD_DATE' values match the expected date format.")
        return True

    def validateNoDuplicateTstampTripID(self):
        """
        Validates that there are no duplicate (TIMESTAMP, EVENT_NO_TRIP) pairs.
        Returns True if valid, False otherwise.
        """
        print("Running validateNoDuplicateTstampTripID...")

        if 'TIMESTAMP' not in self.df.columns or 'EVENT_NO_TRIP' not in self.df.columns:
            print("Missing 'TIMESTAMP' or 'EVENT_NO_TRIP' columns!")
            return False

        duplicated_mask = self.df.duplicated(subset=['TIMESTAMP', 'EVENT_NO_TRIP'], keep=False)

        if duplicated_mask.any():
            print(f"Found {duplicated_mask.sum()} duplicate (TIMESTAMP, EVENT_NO_TRIP) pairs.")
            return False

        print("No duplicate (TIMESTAMP, EVENT_NO_TRIP) pairs found.")
        return True

    def validateLatitudeRange(self):
        """
        Validates that all GPS_LATITUDE values are within the valid range (-90 to 90).
        Returns True if valid, False otherwise.
        """
        print("Running validateLatitudeRange...")

        if 'GPS_LATITUDE' not in self.df.columns:
            print("Missing 'GPS_LATITUDE' column in the dataframe!")
            return False

        out_of_range_mask = (self.df['GPS_LATITUDE'] < -90) | (self.df['GPS_LATITUDE'] > 90)

        if out_of_range_mask.any():
            print(f"Found {out_of_range_mask.sum()} GPS_LATITUDE values outside the valid range.")
            return False

        print("All GPS_LATITUDE values are within the valid range (-90 to 90).")
        return True

    def validateLongitudeRange(self):
        """
        Validates that all GPS_LONGITUDE values are within the valid range (-180 to 180).
        Returns True if valid, False otherwise.
        """
        print("Running validateLongitudeRange...")

        if 'GPS_LONGITUDE' not in self.df.columns:
            print("Missing 'GPS_LONGITUDE' column in the dataframe!")
            return False

        out_of_range_mask = (self.df['GPS_LONGITUDE'] < -180) | (self.df['GPS_LONGITUDE'] > 180)

        if out_of_range_mask.any():
            print(f"Found {out_of_range_mask.sum()} GPS_LONGITUDE values outside the valid range.")
            return False

        print("All GPS_LONGITUDE values are within the valid range (-180 to 180).")
        return True

    def validateSpeedGreaterThanZero(self):
        """
        Validates that all SPEED values are greater than 0.
        Returns True if valid, False otherwise.
        """
        print("Running validateSpeedGreaterThanZero...")

        if 'SPEED' not in self.df.columns:
            print("Missing 'SPEED' column in the dataframe!")
            return False

        invalid_speed_mask = self.df['SPEED'] <= 0

        if invalid_speed_mask.any():
            print(f"Found {invalid_speed_mask.sum()} SPEED values less than or equal to 0.")
            return False

        print("All SPEED values are greater than 0.")
        return True

    def validateSummaryStats(self):
        """
        Performs summary statistics checks on GPS_LATITUDE, GPS_LONGITUDE, and SPEED.
        Returns True if valid, False otherwise.
        """
        print("Running validateSummaryStats...")

        summary_stats = self.df[['GPS_LATITUDE', 'GPS_LONGITUDE', 'SPEED']].describe()

        if not (-90 <= summary_stats.loc['min', 'GPS_LATITUDE'] <= 90 and 
                -90 <= summary_stats.loc['max', 'GPS_LATITUDE'] <= 90):
            print("Latitude values are outside of the valid range (-90, 90).")
            return False

        if not (-180 <= summary_stats.loc['min', 'GPS_LONGITUDE'] <= 180 and 
                -180 <= summary_stats.loc['max', 'GPS_LONGITUDE'] <= 180):
            print("Longitude values are outside of the valid range (-180, 180).")
            return False

        if not (summary_stats.loc['min', 'SPEED'] > 0):
            print("Speed values should be greater than 0.")
            return False

        print("Summary statistics are within expected ranges.")
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