from big_fiubrother_core import StoppableThread
from big_fiubrother_core.messages import SampledFrameMessage
from big_fiubrother_core.image_processing_helper import image_to_bytes
from cv2 import VideoCapture


class SamplingThread(StoppableThread):

    def __init__(self, configuration, input_queue, output_queue):
        super().__init__()
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.sampling_rate = configuration['rate']

    def _execute(self):
        message = self.input_queue.get()

        if message is not None:
            cap = VideoCapture(message['path'])
            frames = []

            while True:
                ret, frame = cap.read()

                if ret:
                    frames.append(frame)
                else:
                    break

            step = round(len(frames)/self.sampling_rate)

            for i in range(self.sampling_rate):
                offset = i*step

                frame_message = SampledFrameMessage(chunk_id=message.id,
                                                    offset=offset,
                                                    frame=image_to_bytes(frame[offset]))
                
                self.output_queue.put(message)

    def _stop(self):
        self.queue.put(None)