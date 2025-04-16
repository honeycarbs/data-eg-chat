import os
import json
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError
from datetime import datetime

def fetch():
    project_id = "data-engineering-455419"
    subscription_id = "Breadcrumb_Storage-sub"
    timeout = 300.0

    messages = []

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        messages.append(message.data.decode('utf-8'))
        message.ack()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

    with subscriber:
        try:
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()
            streaming_pull_future.result()
    today_date = datetime.now().strftime('%Y-%m-%d')
    filename = f"data-{today_date}.json"
    data_to_save = {
        "message_count": len(messages),
        "messages": messages
    }
    with open(filename, "w") as f:
        json.dump(data_to_save, f, indent=2)

    print(f"{len(messages)} messages saved to {filename}.")

def main():
    fetch()


if __name__ == "__main__":
    main()