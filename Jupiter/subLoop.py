import json
import os
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError
from datetime import datetime
import pandas as pd
from transformer import Transformer
from dataValidation import Validation
from insert import DataFrameSQLInserter
from json import load, JSONDecodeError

# This subscriber purpose is to loop on our VM to caputer all data published

def fetch():
    project_id = "data-engineering-455419"
    subscription_id = "Breadcrumb_Storage-sub"
    timeout = 1800.0

    messages = []

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        messages.append(message.data.decode('utf-8'))
        message.ack()

    filename = os.path.join(script_dir, f"data-{today_date}.json")
    while True:
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(project_id, subscription_id)
        streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
        print(f"Listening for messages on {subscription_path}..\n")

        with subscriber:
            try:
                streaming_pull_future.result(timeout=timeout)
            except TimeoutError:
                streaming_pull_future.cancel()
                streaming_pull_future.result()
        today_date = datetime.now().strftime('%Y-%m-%d')
        script_dir = os.path.dirname(os.path.abspath(__file__))
        data_to_save = {
            "message_count": len(messages),
            "messages": messages
        }
        with open(filename, "w") as f:
            json.dump(data_to_save, f, indent=2)

        print(f"{len(messages)} messages saved to {filename}.")
        validateTransformLoad(filename)


def validateTransformLoad(file_path):
    """
    From Cameron's file
    """

    try:
        with open(file_path, 'r') as file:
            data = load(file)

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

        # Instantiate the Validator
        validator = Validation(df)
        validator.validateBeforeTransform()

        validated_df = validator.get_dataframe()

        transformer = Transformer(validated_df)
        transformer.transform()
        transformed_df = transformer.get_dataframe()
        print(transformed_df.head())


        db_uri = "postgresql://postgres:password@localhost/postgres"

        def prepare_for_trip_table(df):
            trip_df = df[['trip_id', 'vehicle_id']].copy()

            trip_df = trip_df.drop_duplicates(subset=['trip_id'])
            return trip_df
        
        def prepare_for_breadcrumb_table(df):
            breadcrumb_df = df[['tstamp', 'latitude', 'longitude', 'speed', 'trip_id']].copy()

            return breadcrumb_df
        
        dataframe_trip = prepare_for_trip_table(transformed_df)
        dataframe_breadcrumb = prepare_for_breadcrumb_table(transformed_df)

        with DataFrameSQLInserter(db_uri) as inserter:
            inserter.insert_dataframe(dataframe_trip, "trip")
            inserter.insert_dataframe(dataframe_breadcrumb, "breadcrumb")


    except Exception as e:
        print(e)


def main():
    fetch()


if __name__ == "__main__":
    main()