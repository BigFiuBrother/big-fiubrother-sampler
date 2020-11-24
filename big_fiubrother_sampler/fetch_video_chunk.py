from big_fiubrother_core import QueueTask
from os import path


class StoreVideoChunk(QueueTask):

    def __init__(self, configuration, input_queue, output_queue):
        super().__init__(input_queue)
        self.output_queue = output_queue
        self.configuration = configuration

    def execute_with(self, message):
        # Write file to process it later
        filepath = path.join('tmp', f"{message.camera_id}_{message.timestamp}.h264")

        with open(filepath, 'wb') as file:
            file.write(message.payload)

        self.output_queue.put({
            'camera_id': message.camera_id,
            'timestamp': message.timestamp,
            'filepath': filepath
        })