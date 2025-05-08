import psycopg2
import pandas as pd
import json
from datetime import datetime

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
    
    def start(self):
        """
        Runs all data validations.
        Raises errors on any validation failure.
        Returns the validated DataFrame if all checks pass.
        """
        self.validateTstamp()
        self.validateNoDuplicateTstampTripID()
        self.validateLatitudeRange()
        self.validateLongitudeRange()
        self.validateSpeedGreaterThanZero()
        self.validateTripIDExists()
        self.validateSummaryStats()
        self.validateSpeedDistribution()
        self.validateReferentialIntegrityWithTrip()
        self.validateDirection()

        return self.df

    def validateTstamp(self):
        """
        Validates that 'tstamp' column exists and all values are datetime.datetime objects.
        Raises ValueError if any invalid entries are found.
        """
        print("Running validateTstamp...")

        if 'tstamp' not in self.df.columns:
            raise ValueError("Missing 'tstamp' column in the dataframe!")

        # Mask for non-datetime entries, term mask is used because it highlights specific entries
        invalid_mask = ~self.df['tstamp'].apply(lambda x: isinstance(x, datetime)) # checks each value in tstamp column
        if invalid_mask.any(): # checks if there's at least one invalid value
            invalid_count = invalid_mask.sum()
            sample_invalid = self.df.loc[invalid_mask, 'tstamp'].head(5)
            raise ValueError(
                f"Found {invalid_count} invalid 'tstamp' entries (not datetime). "
                f"Sample invalid values:\n{sample_invalid.to_string(index=False)}"
            )

        print("All 'tstamp' values are valid datetime objects.")

    def validateNoDuplicateTstampTripID(self):
        """
        Validates that there are no duplicate (tstamp, trip_id) pairs.
        Raises ValueError if duplicates are found.
        """
        print("Running validateNoDuplicateTstampTripID...")

        if 'tstamp' not in self.df.columns or 'trip_id' not in self.df.columns:
            raise ValueError("DataFrame must contain both 'tstamp' and 'trip_id' columns!")

        # Identify duplicated rows based on the (tstamp, trip_id) combination
        duplicated_mask = self.df.duplicated(subset=['tstamp', 'trip_id'], keep=False)

        if duplicated_mask.any(): # boolean series that's True where a row is a duplicate pair
            dup_count = duplicated_mask.sum() # gives the number of duplicated rows
            sample_duplicates = self.df.loc[duplicated_mask, ['tstamp', 'trip_id']].head(5)
            raise ValueError(
                f"Found {dup_count} duplicate (tstamp, trip_id) pairs.\n"
                f"Sample duplicates:\n{sample_duplicates.to_string(index=False)}"
            )

        print("No duplicate (tstamp, trip_id) pairs found.")

    def validateLatitudeRange(self):
        """
        Validates that all latitude values are within the valid range (-90 to 90).
        Raises ValueError if any values are out of range.
        """
        print("Running validateLatitudeRange...")

        if 'latitude' not in self.df.columns:
            raise ValueError("Missing 'latitude' column in the dataframe!")

        # Identify rows with latitude values outside the valid range (-90 to 90)
        out_of_range_mask = (self.df['latitude'] < -90) | (self.df['latitude'] > 90)

        if out_of_range_mask.any():
            out_of_range_count = out_of_range_mask.sum()
            sample_out_of_range = self.df.loc[out_of_range_mask, ['latitude']].head(5)
            raise ValueError(
                f"Found {out_of_range_count} latitude values outside the valid range (-90 to 90).\n"
                f"Sample out-of-range values:\n{sample_out_of_range.to_string(index=False)}"
            )

        print("All latitude values are within the valid range (-90 to 90).")

    def validateLongitudeRange(self):
        """
        Validates that all longitude values are within the valid range (-180 to 180).
        Raises ValueError if any values are out of range.
        """
        print("Running validateLongitudeRange...")

        if 'longitude' not in self.df.columns:
            raise ValueError("Missing 'longitude' column in the dataframe!")

        # Identify rows with longitude values outside the valid range (-180 to 180)
        out_of_range_mask = (self.df['longitude'] < -180) | (self.df['longitude'] > 180)

        if out_of_range_mask.any():
            out_of_range_count = out_of_range_mask.sum()
            sample_out_of_range = self.df.loc[out_of_range_mask, ['longitude']].head(5)
            raise ValueError(
                f"Found {out_of_range_count} longitude values outside the valid range (-180 to 180).\n"
                f"Sample out-of-range values:\n{sample_out_of_range.to_string(index=False)}"
            )

        print("All longitude values are within the valid range (-180 to 180).")

    def validateSpeedGreaterThanZero(self):
        """
        Validates that all speed values are greater than 0.
        Raises ValueError if any values are less than or equal to 0.
        """
        print("Running validateSpeedGreaterThanZero...")

        if 'speed' not in self.df.columns:
            raise ValueError("Missing 'speed' column in the dataframe!")

        # Identify rows with speed values less than or equal to 0
        invalid_speed_mask = self.df['speed'] <= 0

        if invalid_speed_mask.any():
            invalid_speed_count = invalid_speed_mask.sum()
            sample_invalid_speeds = self.df.loc[invalid_speed_mask, ['speed']].head(5)
            raise ValueError(
                f"Found {invalid_speed_count} speed values less than or equal to 0.\n"
                f"Sample invalid values:\n{sample_invalid_speeds.to_string(index=False)}"
            )

        print("All speed values are greater than 0.")

    def validateTripIDExists(self, trip_table):
        """
        Ensures that the trip_id in the Breadcrumb table exists in the Trip table.
        Assumes the Trip table is passed as a DataFrame.
        """
        print("Running validateTripIDExists...")

        if 'trip_id' not in self.df.columns:
            raise ValueError("Missing 'trip_id' column in the dataframe!")

        # Find trip_ids that don't exist in the Trip table
        invalid_trip_ids = self.df[~self.df['trip_id'].isin(trip_table['trip_id'])]
        
        if not invalid_trip_ids.empty:
            invalid_count = invalid_trip_ids.shape[0]
            sample_invalid_trip_ids = invalid_trip_ids[['trip_id']].head(5)
            raise ValueError(
                f"Found {invalid_count} invalid trip_id(s) that do not exist in the Trip table.\n"
                f"Sample invalid trip_ids:\n{sample_invalid_trip_ids.to_string(index=False)}"
            )

        print("All trip_ids exist in the Trip table.")

    def validateSummaryStats(self):
        """
        Performs summary statistics checks on latitude, longitude, and speed.
        Raises ValueError if any values seem anomalous or fall outside expected bounds.
        """
        print("Running validateSummaryStats...")

        # Check summary statistics for 'latitude', 'longitude', and 'speed'
        summary_stats = self.df[['latitude', 'longitude', 'speed']].describe()

        # Checking if latitude, longitude, and speed are within realistic bounds
        if not (-90 <= summary_stats.loc['min', 'latitude'] <= 90 and 
                -90 <= summary_stats.loc['max', 'latitude'] <= 90):
            raise ValueError("Latitude values are outside of the valid range (-90, 90) in the dataset.")

        if not (-180 <= summary_stats.loc['min', 'longitude'] <= 180 and 
                -180 <= summary_stats.loc['max', 'longitude'] <= 180):
            raise ValueError("Longitude values are outside of the valid range (-180, 180) in the dataset.")

        if not (summary_stats.loc['min', 'speed'] > 0):
            raise ValueError("Speed values should be greater than 0 in the dataset.")

        print("Summary statistics are within expected ranges.")

    def validateSpeedDistribution(self):
        """
        Checks if the distribution of speed values looks reasonable.
        Flags speeds that are more than 3 standard deviations away from the mean.
        """
        print("Running validateSpeedDistribution...")

        mean_speed = self.df['speed'].mean()
        std_speed = self.df['speed'].std()

        # Check for any speeds that are more than 3 standard deviations from the mean
        outlier_mask = (self.df['speed'] < (mean_speed - 3 * std_speed)) | (self.df['speed'] > (mean_speed + 3 * std_speed))

        if outlier_mask.any():
            outlier_count = outlier_mask.sum()
            sample_outliers = self.df.loc[outlier_mask, ['speed']].head(5)
            raise ValueError(
                f"Found {outlier_count} outlier speed values that are more than 3 standard deviations from the mean.\n"
                f"Sample outlier speeds:\n{sample_outliers.to_string(index=False)}"
            )

        print("Speed values have a reasonable distribution.")

    def validateReferentialIntegrityWithTrip(self, trip_table):
        """
        Ensures that the trip_id in the BreadCrumb table exists in the Trip table.
        """
        print("Running validateReferentialIntegrityWithTrip...")

        if 'trip_id' not in self.df.columns:
            raise ValueError("Missing 'trip_id' column in the dataframe!")

        # Check for invalid trip_ids that don't exist in the Trip table
        invalid_trip_ids = self.df[~self.df['trip_id'].isin(trip_table['trip_id'])]

        if not invalid_trip_ids.empty:
            invalid_count = invalid_trip_ids.shape[0]
            sample_invalid_trip_ids = invalid_trip_ids[['trip_id']].head(5)
            raise ValueError(
                f"Found {invalid_count} invalid trip_id(s) that do not exist in the Trip table.\n"
                f"Sample invalid trip_ids:\n{sample_invalid_trip_ids.to_string(index=False)}"
            )

        print("All trip_ids exist in the Trip table.")


    def validateDirection(self):
        """
        Ensures that the 'direction' column only contains valid values (1 or 0).
        Raises ValueError if any invalid direction values are found.
        """
        print("Running validateDirection...")

        if 'direction' not in self.df.columns:
            raise ValueError("Missing 'direction' column in the dataframe!")

        # Mask for invalid direction values (i.e., values other than 0 or 1)
        invalid_direction_mask = ~self.df['direction'].isin([0, 1])

        if invalid_direction_mask.any():
            invalid_count = invalid_direction_mask.sum()
            sample_invalid = self.df.loc[invalid_direction_mask, 'direction'].head(5)
            raise ValueError(
                f"Found {invalid_count} invalid 'direction' values (not 0 or 1).\n"
                f"Sample invalid values:\n{sample_invalid.to_string(index=False)}"
            )

        print("All direction values are valid (either 1 or 0).")

"""
def fetch_data_from_db():
    # Connect to PostgreSQL
    conn = psycopg2.connect(
		host="localhost",
		database="postgres",
		user="postgres",
		password="password",
	)

    # SQL query to fetch data from the BreadCrumb table
    sql_query = "SELECT * FROM BreadCrumb;"
    
    # Fetch data and load it into a DataFrame
    df = pd.read_sql(sql_query, conn)

    # Close the connection
    conn.close()

    return df


# Main function to test the validations
def main():
    # Fetch the data from the database
    df = fetch_data_from_db()
    
    # Pass the DataFrame into the Validation class
    validation = Validation(df)

    # Run the validations
    try:
        validated_df = validation.start()  # run all validations
        print("Validation completed successfully.")
        print(validated_df.head())  # print the first few rows of the validated DataFrame
    except ValueError as e:
        print(f"Validation failed: {e}")


if __name__ == "__main__":
    main()

"""