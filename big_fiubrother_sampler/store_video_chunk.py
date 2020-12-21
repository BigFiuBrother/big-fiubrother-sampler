from big_fiubrother_core import QueueTask
from big_fiubrother_core.db import Database, VideoChunk, Frame
from big_fiubrother_core.storage import video_chunks
from big_fiubrother_core.synchronization import ProcessSynchronizer
from big_fiubrother_core.messages import FrameMessage
import requests
import logging


class StoreVideoChunk(QueueTask):

    def __init__(self, configuration, input_queue, output_queue):
        super().__init__(input_queue)
        self.output_queue = output_queue
        self.configuration = configuration

    def init(self):
        self.db = Database(self.configuration['db'])
        self.storage = video_chunks(self.configuration['storage'])
        self.process_synchronizer = ProcessSynchronizer(self.configuration['synchronization'])
        self.web_server_url = self.configuration['web_server_url']

    def execute_with(self, message):
        # Create video chunk and frames in database
        video_chunk = VideoChunk(camera_id=message['camera_id'],
                                 timestamp=message['timestamp'],
                                 frame_count=message['frame_count'],
                                 duration_ms=message['duration'],
                                 frames=[Frame(offset=frame['offset']) for frame in message['frames']])

        self.db.add(video_chunk)

        logging.info(f"{video_chunk.id} with frames created in DB")

        # Register video task in process synchronizer
        self.process_synchronizer.register_video_task(video_chunk.id)

        for frame in video_chunk.frames:
            self.process_synchronizer.register_frame_task(video_chunk.id, frame.id)

        # Create messages to start processing
        for frame_entity, frame_message in zip(video_chunk.frames, message['frames']):
            self.output_queue.put(
                FrameMessage(
                    video_chunk_id=video_chunk.id,
                    frame_id=frame_entity.id,
                    payload=frame_message['payload'])
            )

        # Upload chunk to S3
        self.storage.store_file(f"{video_chunk.id}.mp4", message['filepath'])

        # Notify to web server that new video chunk is ready
        try:
            requests.post(self.web_server_url + str(video_chunk.id))

            logging.info("Sent {} to web server".format(video_chunk.id))
        except requests.exceptions.RequestException as e:
            logging.error(e)

    def close(self):
        self.db.close()
        self.process_synchronizer.close()
