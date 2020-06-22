from big_fiubrother_core.utils import Task, image_to_bytes
from cv2 import VideoCapture
import os


class SampleVideoChunk(Task):

    def __init__(self, configuration):
        self.sampling_rate = configuration['sampling_rate']

    def execute(self, message):
        cap = VideoCapture(message['path'])

        frames = []
        
        while True:
            ret, frame = cap.read()

            if ret:
                frames.append(frame)
            else:
                break

        os.remove(message['path'])

        step = round(len(frames) / self.sampling_rate)

        messages = []
        
        for i in range(self.sampling_rate):
            messages.append({
                'offset': i * step,
                'video_chunk_id': message['video_chunk_id'],
                'payload': image_to_bytes(frames[offset])
            })

        return messages
