#!/usr/bin/env python3

from queue import Queue
from big_fiubrother_core import setup
from big_fiubrother_core import SignalHandler
from big_fiubrother_sampler import VideoChunkConsumer, SamplingThread


if __name__ == "__main__":
    configuration = setup('Big Fiubrother Sampler Application')

    print('[*] Configuring big-fiubrother-sampler')

    consumer_to_persitance_queue = Queue()
    persistance_to_sampler_queue = Queue()
    sampler_to_producer_queue = Queue()

    persitance_thread = PersistanceThread(configuration['persitance'], consumer_to_persitance_queue, persistance_to_sampler_queue)
    sampling_thread = SamplingThread(configuration['sampling'], persistance_to_sampler_queue, sampler_to_producer_queue)
    consumer = VideoChunkConsumer(configuration['consumer'], consumer_to_persitance_queue)
    signal_handler = SignalHandler(callback=consumer.stop)

    print('[*] Configuration finished. Starting big-fiubrother-sampler!')

    sampling_thread.start()
    consumer.start()

    persitance_thread.stop()
    sampling_thread.stop()
    
    persitance_thread.wait()
    sampling_thread.wait()

    print('[*] big-fiubrother-sampler stopped!')