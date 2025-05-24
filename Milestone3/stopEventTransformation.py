import pandas as pd

class stopEventTransformer:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def transform(self):
        self.dropDuplicates()
        self.renameColumns()
        self.renameServiceKeyValues()
        self.renameDirectionValues()
        return self.df
    
    def dropDuplicates(self):
        self.df = self.df.drop_duplicates(subset=['trip_id'])

    def renameColumns(self):
        self.df.rename(columns={'vehicle_number': 'vehicle_id'}, inplace=True)
        self.df.rename(columns={'route_number': 'route_id'}, inplace=True)

    def renameServiceKeyValues(self):
        '''
        Renames service_key values to their respective DB schema enum
        We expect the values: 'S', 'U', 'W', & special case of 'M' (MLK day)
        Convert these values to the following: "Weekday", "Saturday", "Sunday"
        '''
        service_key_map = {
            'S': 'Saturday',
            'U': 'Sunday',
            'W': 'Weekday',
            'M': 'Weekday',
        }

        # Check if 'service_key' column exists in the dataframe
        if 'service_key' in self.df.columns:
            # Replace the values in the 'service_key' column
            self.df['service_key'] = self.df['service_key'].replace(service_key_map)
        else:
            print("Error: 'service_key' column not found in dataframe")

    def renameDirectionValues(self):
        '''
        Renames direction values to their respective DB schema enum
        We expect the values: "0", "1"
        Converts these values to the following: "Out", "Back"
        '''
        direction_map = {
            '0': 'Out',
            '1': 'Back'
        }
        # Check if 'direction' column exists in the dataframe
        if 'direction' in self.df.columns:
            # Replace the values in the 'direction' column
            self.df['direction'] = self.df['direction'].astype(str).replace(direction_map)
        else:
            print("Error: 'direction' column not found in dataframe")
    
