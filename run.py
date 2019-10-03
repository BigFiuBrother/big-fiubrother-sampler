#!/usr/bin/env python3

from queue import Queue
from big_fiubrother_core import setup
from big_fiubrother_core import SignalHandler
from big_fiubrother_sampler import VideoChunkConsumer


if __name__ == "__main__":
    print('[*] Configuring big-fiubrother-sampler')

    configuration = setup('Big Fiubrother Sampler Application')

    queue = Queue()
    consumer = VideoChunkConsumer(configuration['consumer'], queue)
    signal_handler = SignalHandler(callback=consumer.stop)

    print('[*] Configuration finished. Starting big-fiubrother-sampler!')

    consumer.start()

    print('[*] big-fiubrother-sampler stopped!')