#!/usr/bin/env python3

from big_fiubrother_core.utils import setup, Program
from big_fiubrother_core.containers import ThreadContainer
from big_fiubrother_core.containers.communication import (
    MultiThreadQueue,
    RabbitMQPublisher,
    RabbitMQConsumer
)
from big_fiubrother_sampler import (
    StoreVideoChunk,
    SampleVideoChunk,
    StoreFrame
)


if __name__ == "__main__":
    configuration = setup('Big Fiubrother Sampler Application')

    print('[*] Configuring big-fiubrother-sampler')

    queue_1 = MultiThreadQueue()
    queue_2 = MultiThreadQueue()

    program = Program([
        ThreadContainer(
            task=StoreVideoChunk(configuration),
            input_interface=RabbitMQConsumer(configuration['consumer']),
            output_interface=queue_1),
        ThreadContainer(
            task=SampleVideoChunk(configuration),
            input_interface=queue_1,
            output_interface=queue_2),
        ThreadContainer(
            task=SampleVideoChunk(configuration),
            input_interface=queue_2,
            output_interface=RabbitMQPublisher(configuration['publisher']))
    ])

    print('[*] Configuration finished. Starting big-fiubrother-sampler!')

    program.run()

    print('[*] big-fiubrother-sampler stopped!')
