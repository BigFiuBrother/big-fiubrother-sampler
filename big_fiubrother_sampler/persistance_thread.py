from big_fiubrother_core import StoppableThread
from big_fiubrother_core.db import Database
from big_fiubrother_core.db import VideoChunk
from os import path


class PersistanceThread(StoppableThread):

    TMP_PATH = 'tmp' 

    def __init__(self, configuration, input_queue, output_queue):
        super().__init__()
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.path = configuration['tmp_path'] if 'tmp_path' in configuration else self.TMP_PATH
        self.db = Database(configuration['db']) 

    def _execute(self):
        message = self.input_queue.get()

        if message is not None:
            filepath = self._tmp_path(message)

            video_chunk = VideoChunk(camera_id=message.camera_id, timestamp=message.timestamp, payload=message.payload)
            id = self.db.add(video_chunk)

            with open(filepath, 'wb') as file:
                file.write(message.payload)

            self.output_queue.put({'id': video_chunk.id, 'path': filepath})

    def _tmp_path(self, message):
        filename = '{}_{}.h264'.format(message.camera_id, message.timestamp)
        return path.join(self.path, filename)

    def _stop(self):
        self.input_queue.put(None)