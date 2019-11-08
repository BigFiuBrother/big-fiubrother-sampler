#!/usr/bin/env python3

from queue import Queue
from big_fiubrother_core import (
    SignalHandler,
    StoppableThread,
    PublishToRabbitMQ,
    ConsumeFromRabbitMQ,
    setup
)
from big_fiubrother_sampler import (
    StoreVideoChunk,
    SampleVideoChunk
)


if __name__ == "__main__":
    configuration = setup('Big Fiubrother Sampler Application')

    print('[*] Configuring big-fiubrother-sampler')

    consumer_to_storing_queue = Queue()
    storing_to_sampling_queue = Queue()
    sampling_to_publisher_queue = Queue()

    consumer = StoppableThread(
        ConsumeFromRabbitMQ(configuration=configuration['consumer'],
                            output_queue=consumer_to_storing_queue))

    video_storer = StoppableThread(
        StoreVideoChunk(configuration=configuration,
                        input_queue=consumer_to_storing_queue,
                        output_queue=storing_to_sampling_queue))

    video_sampler = StoppableThread(
        SampleVideoChunk(configuration=configuration,
                         input_queue=storing_to_sampling_queue,
                         output_queue=sampling_to_publisher_queue))

    publisher = StoppableThread(
        PublishToRabbitMQ(configuration=configuration['publisher'],
                          input_queue=sampling_to_publisher_queue))

    signal_handler = SignalHandler(callback=consumer.stop)

    print('[*] Configuration finished. Starting big-fiubrother-sampler!')

    publisher.start()
    video_storer.start()
    video_sampler.start()
    consumer.run()

    # Signal Handled STOP
    video_storer.stop()
    video_sampler.stop()
    publisher.stop()

    video_storer.wait()
    video_sampler.wait()
    publisher.wait()

    print('[*] big-fiubrother-sampler stopped!')
