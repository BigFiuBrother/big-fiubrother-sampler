from big_fiubrother_core import QueueTask
from big_fiubrother_core.db import (
    Database,
    VideoChunk
)
from os import path


class StoreVideoChunk(QueueTask):

    TMP_PATH = 'tmp'

    def __init__(self, configuration, input_queue, output_queue):
        super().__init__(input_queue)
        self.output_queue = output_queue
        self.configuration = configuration

    def init(self):
        if 'tmp_path' in self.configuration:
            self.path = self.configuration['tmp_path']
        else:
            self.path = self.TMP_PATH

        self.db = Database(self.configuration['db'])

    def execute_with(self, message):
        video_chunk = VideoChunk(camera_id=message.camera_id,
                                 timestamp=message.timestamp,
                                 payload=message.payload)

        id = self.db.add(video_chunk)

        filepath = self.tmp_video_filepath(message)

        with open(filepath, 'wb') as file:
            file.write(message.payload)

        self.output_queue.put({'id': video_chunk.id, 'path': filepath})

    def tmp_video_filepath(self, message):
        filename = '{}_{}.h264'.format(message.camera_id, message.timestamp)
        return path.join(self.path, filename)
