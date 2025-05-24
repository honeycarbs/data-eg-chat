from google.cloud import pubsub_v1

class PubSubPublisher:
    def __init__(self, project_id, topic_id, max_bytes=1024 * 1024, max_latency=0.01, max_messages=1000):
        self.batch_settings = pubsub_v1.types.BatchSettings(
            max_bytes=max_bytes,
            max_latency=max_latency,
            max_messages=max_messages,
        )
        self.publisher = pubsub_v1.PublisherClient(batch_settings=self.batch_settings)
        self.topic_path = self.publisher.topic_path(project_id, topic_id)

    def publish(self, msg):
        data = msg.encode("utf-8")
        future = self.publisher.publish(self.topic_path, data)
        future.add_done_callback(self._future_callback)
        return future

    def _future_callback(self, future):
        try:
            future.result()
        except Exception as e:
            print(f"An error occurred: {e}")
