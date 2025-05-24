import json
import os
from datetime import datetime
import pandas as pd
from google.cloud import pubsub_v1, storage
from concurrent.futures import TimeoutError
from insert import DataFrameSQLInserter
from stopEventValidation import StopEventValidator
from ast import literal_eval 

class GCSUploader:
    def __init__(self, bucket_name):
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(bucket_name)

    def upload(self, source_file_path, destination_blob_name):
        blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_path)
        print(f"File {source_file_path} uploaded to {destination_blob_name}.")
        return f"gs://{self.bucket.name}/{destination_blob_name}"
    
class StopEventPipeline:
    def __init__(self, db_uri):
        self.db_uri = db_uri

    def validate_load(self, raw_messages):
        try:
            parsed_messages = [literal_eval(msg) for msg in raw_messages]
            df = pd.DataFrame(parsed_messages)

            stopEvent = StopEventValidator(df)
            validated_df = stopEvent.validate()
            validated_df = validated_df.drop_duplicates(subset=['trip_id'])


            with DataFrameSQLInserter(self.db_uri) as inserter:
                inserter.insert_dataframe(validated_df, "trip")

        except Exception as e:
            print(f"Error in validate_load: {e}")

class PubSubFetcher:
    def __init__(self, project_id, subscription_id, bucket_name, db_uri, timeout=1800.0):
        self.project_id = project_id
        self.subscription_id = subscription_id
        self.timeout = timeout
        self.uploader = GCSUploader(bucket_name)
        self.pipeline = StopEventPipeline(db_uri)
        self.messages = []

    def _callback(self, message: pubsub_v1.subscriber.message.Message):
        self.messages.append(message.data.decode('utf-8'))
        message.ack()

    def fetch_and_process(self):
        while True:
            today_date = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            filename = f"data-{today_date}.json"
            gcs_filename = f"breadcrumb_data/{filename}"

            subscriber = pubsub_v1.SubscriberClient()
            subscription_path = subscriber.subscription_path(self.project_id, self.subscription_id)
            streaming_pull_future = subscriber.subscribe(subscription_path, callback=self._callback)
            print(f"Listening for messages on {subscription_path}..\n")

            with subscriber:
                try:
                    streaming_pull_future.result(timeout=self.timeout)
                except TimeoutError:
                    streaming_pull_future.cancel()
                    streaming_pull_future.result()

            data_to_save = {
                "message_count": len(self.messages),
                "messages": self.messages
            }

            with open(filename, "w") as f:
                json.dump(data_to_save, f, indent=2)

            print(f"{len(self.messages)} messages saved to {filename}.")

            # Upload to GCS
            gcs_path = self.uploader.upload(filename, gcs_filename)

            # Validate & load to DB
            self.pipeline.validate_load(self.messages)

            # Clean up
            os.remove(filename)
            print(f"Deleted local file: {filename}")
            self.messages.clear()

if __name__ == "__main__":
    project_id = "data-engineering-455419"
    subscription_id = "Stop-Event-Data-sub"
    bucket_name = "jakira-stop-bucket"
    db_uri = os.getenv("DB_URI")

    fetcher = PubSubFetcher(project_id, subscription_id, bucket_name, db_uri)
    fetcher.fetch_and_process()