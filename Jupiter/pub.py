import os
from google.cloud import pubsub_v1

def publish(msg):
    project_id = "data-engineering-455419"
    topic_id = "Breadcrumb_Storage"
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    
    data_str = msg
    data = data_str.encode("utf-8")
    future = publisher.publish(topic_path, data)
    print(future.result())

    print(f"Published messages to {topic_path}.")