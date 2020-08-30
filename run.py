#!/usr/bin/env python3

from queue import Queue
from big_fiubrother_core import (
    StoppableThread,
    PublishToRabbitMQ,
    ConsumeFromRabbitMQ,
    runtime_context,
    run
)
from big_fiubrother_sampler import (
    StoreVideoChunk,
    SampleVideoChunk,
    StoreFrame
)


if __name__ == "__main__":
    with runtime_context('Big Fiubrother Sampler Application') as configuration:
        print('[*] Configuring big-fiubrother-sampler')

        queue_1 = Queue()
        
        consumer = StoppableThread(
            ConsumeFromRabbitMQ(configuration=configuration['consumer'],
                                output_queue=queue_1))

        queue_2 = Queue()

        video_storer = StoppableThread(
            StoreVideoChunk(configuration=configuration,
                            input_queue=queue_1,
                            output_queue=queue_2))

        queue_3 = Queue()

        video_sampler = StoppableThread(
            SampleVideoChunk(configuration=configuration,
                             input_queue=queue_2,
                             output_queue=queue_3))

        queue_4 = Queue()

        frame_storer = StoppableThread(
            StoreFrame(configuration=configuration,
                            input_queue=queue_3,
                            output_queue=queue_4))

        publisher = StoppableThread(
            PublishToRabbitMQ(configuration=configuration['publisher'],
                              input_queue=queue_4))

        print('[*] Configuration finished. Starting big-fiubrother-sampler!')

        run([
            consumer,
            video_storer,
            video_sampler,
            frame_storer,
            publisher
        ])

        print('[*] big-fiubrother-sampler stopped!')
