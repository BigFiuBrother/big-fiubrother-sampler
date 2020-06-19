from big_fiubrother_core import Task
from big_fiubrother_core.db import Database, Frame
from big_fiubrother_core.messages import FrameMessage
from big_fiubrother_core.synchronization import ProcessSynchronizer


class StoreFrame(Task):

    def __init__(self):
        self.db = Database(configuration['db'])
        self.process_synchronizer = ProcessSynchronizer(configuration['synchronization']) 


    def execute(self, message):
        frame = Frame(
            offset=message["offset"],
            video_chunk_id=message['video_chunk_id'])

        self.db.add(frame)

        self.process_synchronizer.register_frame_task(
            str(frame.id), str(message['video_chunk_id']))

        return FrameMessage(
            video_chunk_id=message['video_chunk_id'],
            frame_id=frame.id,
            payload=message['payload'])

    def close(self):
        self.db.close()
        self.process_synchronizer.close()
