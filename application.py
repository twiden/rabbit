import pika
import time


class AcknowledgedCallback(object):

    def __init__(self, routing_key, callback_handler, channel):
        self.routing_key = routing_key
        self.callback_handler = callback_handler
        self.channel = channel

    def handle(self, ch, method, properties, body):
        self.callback_handler(message=body, subject=self.routing_key)
        self.channel.basic_ack(delivery_tag=method.delivery_tag)


class RabbitMqExchange(object):

    def __init__(self, name, host):
        self.connection = None
        self.type = 'topic'
        self.name = name
        self.host = host

    def initialize(self):
        if self.connection:
            raise Exception('Can only be initialized once')

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))

    def on_new_channel(self, func):
        channel = self.connection.channel()
        channel.basic_qos(prefetch_count=1)
        channel.exchange_declare(exchange=self.name, type=self.type)
        func(channel, self.name)

    def close(self):
        self.connection.close()


class RabbitMq(object):

    def __init__(self, routing_key, queue, rabbit_mq_exchange):
        self.routing_key = routing_key
        self.queue = queue
        self.rabbit_mq_exchange = rabbit_mq_exchange
        self.rabbit_mq_exchange.initialize()

    def publish(self, message):
        PERSISTANT = 2

        def basic_publish(channel, exchange):
            channel.basic_publish(
                exchange=exchange,
                routing_key=self.routing_key,
                body=message,
                properties=pika.BasicProperties(delivery_mode=PERSISTANT))

        self.rabbit_mq_exchange.on_new_channel(basic_publish)

    def consume(self, callback):

        def basic_consume(channel, exchange):
            channel.queue_declare(queue=self.queue, durable=True)
            channel.queue_bind(exchange=exchange, queue=self.queue, routing_key=self.routing_key)
            ack_handler = AcknowledgedCallback(self.routing_key, callback, channel)
            channel.basic_consume(ack_handler.handle, queue=self.queue)
            channel.start_consuming()

        self.rabbit_mq_exchange.on_new_channel(basic_consume)

    def close(self):
        self.rabbit_mq_exchange.close()


classner sendir ocnrecevier sublassar rabbet

class Producer(object):

    def __init__(self, id, n, rabbit_mq):
        self.id = id
        self.n = n
        self.rabbit_mq = rabbit_mq
        self.total_sent = 0

    def run(self):
        print 'Started producer %s' % self.id

        for i in xrange(self.n):
            self.rabbit_mq.publish('%s:%s' % (self.id, i))
            self.total_sent += 1
            print 'Producer %s\tsent "%s:%s"\ttotal_sent %s' % (self.id, self.id, i, self.total_sent)
            time.sleep(1)

        self.rabbit_mq.close()


class Consumer(object):

    def __init__(self, id, sleep, rabbit_mq):
        self.id = id
        self.sleep = sleep
        self.rabbit_mq = rabbit_mq
        self.total_received = 0

    def run(self):
        print 'Started consumer %s' % self.id
        self.rabbit_mq.consume(self.callback)
        self.rabbit_mq.close()

    def callback(self, message, subject=None):
        self.total_received += 1
        print 'Consumer %s\treceived "%s"\ttotal_received %s' % (self.id, message, self.total_received)
        time.sleep(self.sleep)