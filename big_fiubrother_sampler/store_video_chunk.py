from big_fiubrother_core.utils import Task
from big_fiubrother_core.db import Database, VideoChunk
from big_fiubrother_core.storage import S3Client
from big_fiubrother_core.synchronization import ProcessSynchronizer
from os import path


class StoreVideoChunk(Task):

    def __init__(self, configuration):
        self.db = Database(configuration['db'])
        self.storage = S3Client(configuration['storage'])
        self.process_synchronizer = ProcessSynchronizer(
            configuration['synchronization'])

    def execute(self, message):        
        video_chunk = VideoChunk(camera_id=message.camera_id,
                                 timestamp=message.timestamp)

        self.db.add(video_chunk)

        filepath = path.join('tmp', '{}.h264'.format(video_chunk.id))
        
        with open(filepath, 'wb') as file:
            file.write(message.payload)
        
        self.storage.store_file(str(video_chunk.id), filepath)

        self.process_synchronizer.register_video_chunk(str(video_chunk.id))

        return {'video_chunk_id': video_chunk.id, 'path': filepath}

    def close(self):
        self.db.close()
        self.process_synchronizer.close()
        