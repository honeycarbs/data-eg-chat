import json
import os
from google.cloud import pubsub_v1, storage
from concurrent.futures import TimeoutError
from datetime import datetime
import pandas as pd
from transformer import Transformer
from dataValidation import Validation
from insert import DataFrameSQLInserter
from json import load

def upload_to_gcs(bucket_name, source_file_path, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)
    print(f"File {source_file_path} uploaded to {destination_blob_name}.")
    return f"gs://{bucket_name}/{destination_blob_name}"

def fetch():
    project_id = "data-engineering-455419"
    subscription_id = "Breadcrumb_Storage-sub"
    timeout = 1800.0
    bucket_name = "jakira-bucket"

    messages = []

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        messages.append(message.data.decode('utf-8'))
        message.ack()

    while True:
        today_date = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        script_dir = os.path.dirname(os.path.abspath(__file__))
        filename = os.path.join(script_dir, f"data-{today_date}.json")
        gcs_filename = f"breadcrumb_data/data-{today_date}.json"

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

        data_to_save = {
            "message_count": len(messages),
            "messages": messages
        }
        with open(filename, "w") as f:
            json.dump(data_to_save, f, indent=2)

        print(f"{len(messages)} messages saved to {filename}.")

        # Upload to GCS
        gcs_path = upload_to_gcs(bucket_name, filename, gcs_filename)

        # Optional: validate/load using local file
        validateTransformLoad(messages)

        # Delete local file
        os.remove(filename)
        print(f"Deleted local file: {filename}")

        # Clear message buffer
        messages.clear()

def validateTransformLoad(raw_messages):
    try:
        # Step 1: Convert list of strings to list of dictionaries
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

        Validation(transformed_df).validateAfterTransform()

        db_uri = os.getenv("DB_URI")

        #dataframe_trip = Transformer.createTripDF(transformed_df)
        dataframe_breadcrumb = Transformer.createBreadcrumbDF(transformed_df)

        with DataFrameSQLInserter(db_uri) as inserter:
            # We no longer insert into 'trip' table after Milestone2.
            #inserter.insert_dataframe(dataframe_trip, "trip")
            inserter.insert_dataframe(dataframe_breadcrumb, "breadcrumb")

    except Exception as e:
        print(f"Error in validateTransformLoad: {e}")

def main():
    fetch()

if __name__ == "__main__":
    main()
