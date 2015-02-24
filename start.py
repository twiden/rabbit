#!/usr/bin/env python

from application import Consumer, Producer
from framework import RabbitMq, RabbitMqClient, RabbitMqConnection
from multiprocessing import Process
import time

config = {
    'messaging': {
        'exchange': {
            'name': 'myexchange',
            'type': 'topic'
        },
        'hosts': [
            '127.0.0.1',
            # '192.168.138.170'
        ]
    }
}


def rabbit_mq_factory():
    return RabbitMqClient('my.durable.topic', 'shared_work_queue', RabbitMq(config, RabbitMqConnection(config)))


if __name__ == '__main__':
    n_producers = input('How many producers? ')
    n_consumers = input('How many consumers? ')
    n_messages = input('How many messages per producer? ')

    producers = []
    consumers = []

    for i in xrange(n_consumers):
        t = Consumer(i, 0, rabbit_mq_factory())
        Process(target=t.run).start()
    time.sleep(10)
    for i in xrange(n_producers):
        t = Producer(i, n_messages, rabbit_mq_factory())
        Process(target=t.run).start()