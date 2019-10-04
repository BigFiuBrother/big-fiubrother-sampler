from big_fiubrother_core.message_clients.rabbitmq import Consumer


class VideoChunkConsumer:

    def __init__(self, configuration, output_queue):
        self.consumer = Consumer(configuration, self._consumer_callback)
        self.output_queue = output_queue
 
    def start(self):
        self.consumer.start()

    def _consumer_callback(self, body):
        self.output_queue.put(body)

    def stop(self):
        self.consumer.stop()