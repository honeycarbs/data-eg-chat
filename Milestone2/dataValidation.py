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
        Raises ValueError if any invalid entries are found.
        """
        print("Running validateTstamp...")

        if 'TIMESTAMP' not in self.df.columns:
            raise ValueError("Missing 'TIMESTAMP' column in the dataframe!")

        # Mask for non-datetime entries, term mask is used because it highlights specific entries
        invalid_mask = ~self.df['TIMESTAMP'].apply(lambda x: isinstance(x, datetime)) # checks each value in TIMESTAMP column
        if invalid_mask.any(): # checks if there's at least one invalid value
            invalid_count = invalid_mask.sum()
            sample_invalid = self.df.loc[invalid_mask, 'TIMESTAMP'].head(5)
            raise ValueError(
                f"Found {invalid_count} invalid 'TIMESTAMP' entries (not datetime). "
                f"Sample invalid values:\n{sample_invalid.to_string(index=False)}"
            )

        print("All 'TIMESTAMP' values are valid datetime objects.")

    def validateDate(self):
        """
        Validates that all 'OPD_DATE' values in the DataFrame match the expected format: DDMMMYYYY:HH:MM:SS.
        Example: '08DEC2022:00:00:00'
        Raises ValueError if any values do not match the expected format.
        """
        print("Running validateDate...")

        if 'OPD_DATE' not in self.df.columns:
            raise ValueError("Missing 'OPD_DATE' column in the dataframe!")

        # Regular expression pattern: 2 digits + 3 uppercase letters + 4 digits + colon + 2 digits + colon + 2 digits + colon + 2 digits
        pattern = re.compile(r"^\d{2}[A-Z]{3}\d{4}:\d{2}:\d{2}:\d{2}$")

        # Apply regex check to each OPD_DATE value
        invalid_mask = ~self.df['OPD_DATE'].astype(str).apply(lambda x: bool(pattern.match(x)))

        if invalid_mask.any():
            invalid_count = invalid_mask.sum()
            sample_invalid = self.df.loc[invalid_mask, 'OPD_DATE'].head(5)
            raise ValueError(
                f"Found {invalid_count} 'OPD_DATE' values that do not match the expected format 'DDMMMYYYY:HH:MM:SS'.\n"
                f"Sample invalid values:\n{sample_invalid.to_string(index=False)}"
            )

        print("All 'OPD_DATE' values match the expected date format.")

    def validateNoDuplicateTstampTripID(self):
        """
        Validates that there are no duplicate (TIMESTAMP, EVENT_NO_TRIP) pairs.
        Raises ValueError if duplicates are found.
        """
        print("Running validateNoDuplicateTstampTripID...")

        if 'TIMESTAMP' not in self.df.columns or 'EVENT_NO_TRIP' not in self.df.columns:
            raise ValueError("DataFrame must contain both 'TIMESTAMP' and 'EVENT_NO_TRIP' columns!")

        # Identify duplicated rows based on the (TIMESTAMP, EVENT_NO_TRIP) combination
        duplicated_mask = self.df.duplicated(subset=['TIMESTAMP', 'EVENT_NO_TRIP'], keep=False)

        if duplicated_mask.any(): # boolean series that's True where a row is a duplicate pair
            dup_count = duplicated_mask.sum() # gives the number of duplicated rows
            sample_duplicates = self.df.loc[duplicated_mask, ['TIMESTAMP', 'EVENT_NO_TRIP']].head(5)
            raise ValueError(
                f"Found {dup_count} duplicate (TIMESTAMP, EVENT_NO_TRIP) pairs.\n"
                f"Sample duplicates:\n{sample_duplicates.to_string(index=False)}"
            )

        print("No duplicate (TIMESTAMP, EVENT_NO_TRIP) pairs found.")

    def validateLatitudeRange(self):
        """
        Validates that all GPS_LATITUDE values are within the valid range (-90 to 90).
        Raises ValueError if any values are out of range.
        """
        print("Running validateLatitudeRange...")

        if 'GPS_LATITUDE' not in self.df.columns:
            raise ValueError("Missing 'GPS_LATITUDE' column in the dataframe!")

        # Identify rows with GPS_LATITUDE values outside the valid range (-90 to 90)
        out_of_range_mask = (self.df['GPS_LATITUDE'] < -90) | (self.df['GPS_LATITUDE'] > 90)

        if out_of_range_mask.any():
            out_of_range_count = out_of_range_mask.sum()
            sample_out_of_range = self.df.loc[out_of_range_mask, ['GPS_LATITUDE']].head(5)
            raise ValueError(
                f"Found {out_of_range_count} GPS_LATITUDE values outside the valid range (-90 to 90).\n"
                f"Sample out-of-range values:\n{sample_out_of_range.to_string(index=False)}"
            )

        print("All GPS_LATITUDE values are within the valid range (-90 to 90).")

    def validateLongitudeRange(self):
        """
        Validates that all GPS_LONGITUDE values are within the valid range (-180 to 180).
        Raises ValueError if any values are out of range.
        """
        print("Running validateLongitudeRange...")

        if 'GPS_LONGITUDE' not in self.df.columns:
            raise ValueError("Missing 'GPS_LONGITUDE' column in the dataframe!")

        # Identify rows with GPS_LONGITUDE values outside the valid range (-180 to 180)
        out_of_range_mask = (self.df['GPS_LONGITUDE'] < -180) | (self.df['GPS_LONGITUDE'] > 180)

        if out_of_range_mask.any():
            out_of_range_count = out_of_range_mask.sum()
            sample_out_of_range = self.df.loc[out_of_range_mask, ['GPS_LONGITUDE']].head(5)
            raise ValueError(
                f"Found {out_of_range_count} GPS_LONGITUDE values outside the valid range (-180 to 180).\n"
                f"Sample out-of-range values:\n{sample_out_of_range.to_string(index=False)}"
            )

        print("All GPS_LONGITUDE values are within the valid range (-180 to 180).")

    def validateSpeedGreaterThanZero(self):
        """
        Validates that all SPEED values are greater than 0.
        Raises ValueError if any values are less than or equal to 0.
        """
        print("Running validateSpeedGreaterThanZero...")

        if 'SPEED' not in self.df.columns:
            raise ValueError("Missing 'SPEED' column in the dataframe!")

        # Identify rows with SPEED values less than or equal to 0
        invalid_speed_mask = self.df['SPEED'] <= 0

        if invalid_speed_mask.any():
            invalid_speed_count = invalid_speed_mask.sum()
            sample_invalid_speeds = self.df.loc[invalid_speed_mask, ['SPEED']].head(5)
            raise ValueError(
                f"Found {invalid_speed_count} SPEED values less than or equal to 0.\n"
                f"Sample invalid values:\n{sample_invalid_speeds.to_string(index=False)}"
            )

        print("All SPEED values are greater than 0.")


    def validateSummaryStats(self):
        """
        Performs summary statistics checks on GPS_LATITUDE, GPS_LONGITUDE, and SPEED.
        Raises ValueError if any values seem anomalous or fall outside expected bounds.
        """
        print("Running validateSummaryStats...")

        # Check summary statistics for 'GPS_LATITUDE', 'GPS_LONGITUDE', and 'SPEED'
        summary_stats = self.df[['GPS_LATITUDE', 'GPS_LONGITUDE', 'SPEED']].describe()

        # Checking if GPS_LATITUDE, GPS_LONGITUDE, and SPEED are within realistic bounds
        if not (-90 <= summary_stats.loc['min', 'GPS_LATITUDE'] <= 90 and 
                -90 <= summary_stats.loc['max', 'GPS_LATITUDE'] <= 90):
            raise ValueError("Latitude values are outside of the valid range (-90, 90) in the dataset.")

        if not (-180 <= summary_stats.loc['min', 'GPS_LONGITUDE'] <= 180 and 
                -180 <= summary_stats.loc['max', 'GPS_LONGITUDE'] <= 180):
            raise ValueError("Longitude values are outside of the valid range (-180, 180) in the dataset.")

        if not (summary_stats.loc['min', 'SPEED'] > 0):
            raise ValueError("Speed values should be greater than 0 in the dataset.")

        print("Summary statistics are within expected ranges.")

    def validateSpeedDistribution(self):
        """
        Checks if the distribution of SPEED values looks reasonable.
        Flags speeds that are more than 3 standard deviations away from the mean.
        """
        print("Running validateSpeedDistribution...")

        mean_speed = self.df['SPEED'].mean()
        std_speed = self.df['SPEED'].std()

        # Check for any speeds that are more than 3 standard deviations from the mean
        outlier_mask = (self.df['SPEED'] < (mean_speed - 3 * std_speed)) | (self.df['SPEED'] > (mean_speed + 3 * std_speed))

        if outlier_mask.any():
            outlier_count = outlier_mask.sum()
            sample_outliers = self.df.loc[outlier_mask, ['SPEED']].head(5)
            raise ValueError(
                f"Found {outlier_count} outlier SPEED values that are more than 3 standard deviations from the mean.\n"
                f"Sample outlier speeds:\n{sample_outliers.to_string(index=False)}"
            )

        print("Speed values have a reasonable distribution.")

    def validateDirection(self):
        """
        Ensures that the 'DIRECTION' column only contains valid values (1 or 0).
        Raises ValueError if any invalid DIRECTION values are found.
        """
        print("Running validateDirection...")

        if 'DIRECTION' not in self.df.columns:
            raise ValueError("Missing 'DIRECTION' column in the dataframe!")

        # Mask for invalid DIRECTION values (i.e., values other than 0 or 1)
        invalid_direction_mask = ~self.df['DIRECTION'].isin([0, 1])

        if invalid_direction_mask.any():
            invalid_count = invalid_direction_mask.sum()
            sample_invalid = self.df.loc[invalid_direction_mask, 'DIRECTION'].head(5)
            raise ValueError(
                f"Found {invalid_count} invalid 'DIRECTION' values (not 0 or 1).\n"
                f"Sample invalid values:\n{sample_invalid.to_string(index=False)}"
            )

        print("All DIRECTION values are valid (either 1 or 0).")
        
    def validateTripIdOneVehicle(self):
        """
        Validates that each EVENT_NO_TRIP is associated with only one VEHICLE_ID.
        Raises ValueError if any EVENT_NO_TRIP is linked to multiple VEHICLE_IDs.
        """
        print("Running validateTripIdOneVehicle...")

        if 'EVENT_NO_TRIP' not in self.df.columns or 'VEHICLE_ID' not in self.df.columns:
            raise ValueError("Missing 'EVENT_NO_TRIP' or 'VEHICLE_ID' column in the dataframe!")

        # Group by EVENT_NO_TRIP and count unique VEHICLE_IDs
        trip_vehicle_counts = self.df.groupby('EVENT_NO_TRIP')['VEHICLE_ID'].nunique()

        # Filter trips with more than one VEHICLE_ID
        multiple_vehicles = trip_vehicle_counts[trip_vehicle_counts > 1]

        if not multiple_vehicles.empty:
            sample_violations = multiple_vehicles.head(5)
            raise ValueError(
                f"Found {len(multiple_vehicles)} EVENT_NO_TRIP values associated with multiple VEHICLE_IDs.\n"
                f"Sample violations:\n{sample_violations.to_string()}"
            )

        print("Each EVENT_NO_TRIP is associated with only one VEHICLE_ID.")