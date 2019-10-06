#!/usr/bin/env python3

from queue import Queue
from big_fiubrother_core import setup
from big_fiubrother_core import SignalHandler
from big_fiubrother_sampler import VideoChunkConsumer, SamplingThread


if __name__ == "__main__":
    configuration = setup('Big Fiubrother Sampler Application')

    print('[*] Configuring big-fiubrother-sampler')

    queue = Queue()
    sampling_thread = SamplingThread(queue)
    consumer = VideoChunkConsumer(configuration['consumer'], queue)
    signal_handler = SignalHandler(callback=consumer.stop)

    print('[*] Configuration finished. Starting big-fiubrother-sampler!')

    sampling_thread.start()
    consumer.start()

    sampling_thread.stop()
    sampling_thread.wait()

    print('[*] big-fiubrother-sampler stopped!')