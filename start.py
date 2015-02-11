#!/usr/bin/env python

from application import RabbitMq, Consumer, Producer, RabbitMqExchange
from multiprocessing import Process
import time

def rabbit_mq_factory():
    return RabbitMq('my.durable.topic', 'topic', 'shared_work_queue', RabbitMqExchange('myexchange', 'localhost'))


if __name__ == '__main__':
    n_producers = input('How many producers? ')
    n_consumers = input('How many consumers? ')
    n_messages = input('How many messages per producer? ')

    producers = []
    consumers = []

    for i in xrange(n_consumers):
        t = Consumer(i, 3, rabbit_mq_factory())
        Process(target=t.run).start()

    # Wait here for a bit. The consumers must get ready
    time.sleep(n_consumers)

    for i in xrange(n_producers):
        t = Producer(i, n_messages, rabbit_mq_factory())
        Process(target=t.run).start()