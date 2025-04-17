from google.cloud import pubsub_v1

batch_settings = pubsub_v1.types.BatchSettings(
    max_bytes=1024 * 1024,
    max_latency=0.01,
    max_messages=1000,
)

publisher = pubsub_v1.PublisherClient(batch_settings=batch_settings)
project_id = "data-engineering-455419"
topic_id = "Breadcrumb_Storage"
topic_path = publisher.topic_path(project_id, topic_id)

def publish(msg):
    data = msg.encode("utf-8")
    future = publisher.publish(topic_path, data)