from big_fiubrother_core import QueueTask
from big_fiubrother_core.db import (
    Database,
    Frame,
    VideoChunkProcess
)
from big_fiubrother_core.messages import FrameMessage
from big_fiubrother_core.utils import image_to_bytes
from cv2 import VideoCapture
import os


class SampleVideoChunk(QueueTask):

    def __init__(self, configuration, input_queue, output_queue):
        super().__init__(input_queue)
        self.output_queue = output_queue
        self.configuration = configuration

    def init(self):
        self.sampling_rate = self.configuration['sampling_rate']
        self.db = Database(self.configuration['db'])

    def execute_with(self, message):
        cap = VideoCapture(message['path'])
        frames = []

        while True:
            ret, frame = cap.read()

            if ret:
                frames.append(frame)
            else:
                break

        video_chunk_process = VideoChunkProcess(
            video_chunk_id=message['id'],
            total_frames_count=self.sampling_rate)

        self.db.add(video_chunk_process)

        step = round(len(frames) / self.sampling_rate)

        for i in range(self.sampling_rate):
            offset = i * step

            frame = Frame(offset=offset,
                          video_chunk_id=message['id'])
            self.db.add(frame)

            payload = image_to_bytes(frames[offset])

            frame_message = FrameMessage(video_chunk_id=message['id'],
                                         frame_id=frame.id,
                                         payload=payload)

            self.output_queue.put(frame_message)

        os.remove(message['path'])
