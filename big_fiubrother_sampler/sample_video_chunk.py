from big_fiubrother_core import QueueTask
from big_fiubrother_core.utils import image_to_bytes
from cv2 import VideoCapture
from .video_builder import VideoBuilder
import os


class SampleVideoChunk(QueueTask):

    def __init__(self, configuration, input_queue, output_queue):
        super().__init__(input_queue)
        self.output_queue = output_queue
        self.sampling_rate = configuration['sampling_rate']
        self.fps = configuration['fps']

    def execute_with(self, message):
        video_capture = VideoCapture(message['filepath'])

        writer = VideoBuilder(filename=f"tmp/{message['camera_id']}_{message['timestamp']}",
                              width=int(video_capture.get(3)),
                              height=int(video_capture.get(4)),
                              fps=self.fps)

        # Read frames to memory and then remove temporary video
        frames = []

        while True:
            ret, frame = video_capture.read()

            if ret:
                frames.append(frame)
                writer.add_frame(frame)
            else:
                break

        os.remove(message['filepath'])

        # Write video as mp4 to disk and add info to message
        writer.close()
        message['filepath'] = writer.filepath
        message['duration'] = writer.duration()
        message['frame_count'] = len(frames)

        # Sample video
        step = round(len(frames) / self.sampling_rate)

        message['frames'] = []

        for i in range(self.sampling_rate):
            offset = i * step

            message['frames'].append({
                'offset': offset,
                'payload': image_to_bytes(frames[offset])
            })

        self.output_queue.put(message)