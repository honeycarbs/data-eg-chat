import pandas as pd

class StopEventValidator:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def validate(self):
        self.validate_columns_exist()
        self.validate_trip_id()
        self.validate_direction()
        self.validate_vehicle_number()
        self.validate_route_number()
        return self.df

    def validate_columns_exist(self):
        """
        Validates that the input DataFrame contains all required columns.

        Required columns:
        - 'trip_id'
        - 'vehicle_number'
        - 'route_number'
        - 'direction'
        - 'service_key'

        Raises:
            AssertionError: If any of the required columns are missing.
        """
        required_columns = {'trip_id', 'vehicle_number', 'route_number', 'direction', 'service_key'}
        missing_columns = required_columns - set(self.df.columns)
        assert not missing_columns, f"Missing required columns: {missing_columns}"

    def validate_trip_id(self):
        """
        Validates that the 'trip_id' column contains no negative values.
        Removes rows with negative 'trip_id' values.
        """
        try:
            assert (self.df['trip_id'] >= 0).all(), "trip_id contains negative values"
        except AssertionError:
            self.df = self.df[self.df['trip_id'] >= 0].reset_index(drop=True)

    def validate_direction(self):
        """
        Validates that all values in the 'direction' column are either 0 or 1.
        Removes rows with invalid direction values.
        """
        try:
            valid_directions = {0, 1}
            invalid_mask = ~self.df['direction'].isin(valid_directions)
            assert not invalid_mask.any(), f"Invalid direction values found: {set(self.df.loc[invalid_mask, 'direction'])}"
        except AssertionError:
            self.df = self.df[self.df['direction'].isin(valid_directions)].reset_index(drop=True)

    def validate_route_number(self):
        """
        Validates that the 'route_number' column contains no negative values.
        Removes rows with negative 'route_number' values.
        """
        try:
            invalid_mask = self.df['route_number'].apply(lambda val: str(val).lstrip('-').isdigit() and int(val) < 0)
            assert not invalid_mask.any(), f"route_number contains negative values: {self.df.loc[invalid_mask, 'route_number'].tolist()}"
        except AssertionError:
            self.df = self.df[~invalid_mask].reset_index(drop=True)

    def validate_vehicle_number(self):
        """
        Validates that the 'vehicle_number' column contains no negative values.
        Removes rows with negative 'vehicle_number' values.
        """
        try:
            invalid_mask = self.df['vehicle_number'].apply(lambda val: str(val).lstrip('-').isdigit() and int(val) < 0)
            assert not invalid_mask.any(), f"vehicle_number contains negative values: {self.df.loc[invalid_mask, 'vehicle_number'].tolist()}"
        except AssertionError:
            self.df = self.df[~invalid_mask].reset_index(drop=True)