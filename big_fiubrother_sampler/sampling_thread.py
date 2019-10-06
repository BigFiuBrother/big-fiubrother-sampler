from big_fiubrother_core.messages import VideoChunkMessage
from big_fiubrother_core.messages import decode_message
from big_fiubrother_core import StoppableThread


class SamplingThread(StoppableThread):

    def __init__(self, queue):
        super().__init__()
        self.queue = queue

    def _execute(self):
        message = self.queue.get()

        if message is not None:
            video_chunk_message = decode_message(message)
            filepath = 'tmp/{}_{}.mp4'.format(video_chunk_message.camera_id, video_chunk_message.timestamp)
            
            with open(filepath, 'wb') as file:
                file.write(video_chunk_message.video_chunk)

    def _stop(self):
        self.queue.put(None)

