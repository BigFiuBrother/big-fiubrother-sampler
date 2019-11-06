from big_fiubrother_core import QueueTask
from big_fiubrother_core.db import (
    Database,
    Frame
)
from big_fiubrother_core.messages import FrameMessage
from big_fiubrother_core.image_processing_helper import image_to_bytes
from cv2 import VideoCapture
import os


class SampleVideoChunk(QueueTask):

    def __init__(self, configuration, input_queue, output_queue):
        super().__init__(input_queue)
        self.output_queue = output_queue
        self.sampling_rate = configuration['samplingrate']

    def init(self):
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

        step = round(len(frames) / self.sampling_rate)

        for i in range(self.sampling_rate):
            offset = i * step

            frame = Frame(offset=offset,
                          video_chunk_id=message['id'])
            frame_id = self.db.add(frame)

            payload = image_to_bytes(frames[offset])
            frame_message = FrameMessage(video_chunk_id=message['id'],
                                         frame_id=frame_id,
                                         payload=payload)

            self.output_queue.put(frame_message)

        os.remove(message['path'])
