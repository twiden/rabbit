import pika
from pika.exceptions import ConnectionClosed
import time
import random
from collections import OrderedDict
import random
import threading
import logging
import time
import sys

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)


class AcknowledgedCallback(object):

    def __init__(self, routing_key, callback_handler, rabbit_mq_connection_manager):
        self.routing_key = routing_key
        self.callback_handler = callback_handler
        self.rabbit_mq_connection_manager = rabbit_mq_connection_manager

    def handle(self, ch, method, properties, body):
        self.callback_handler(message=body, subject=self.routing_key)
        ch.basic_ack(delivery_tag=method.delivery_tag)


class NoHostsAvailable(Exception):
    pass


class CouldNotDeliver(Exception):
    pass


class RabbitMq(object):
    """
    Manages a set of configs for live RabbitMQ hosts, by pinging them at regular intervals
    """
    def __init__(self, config):
        self.config = config.get('messaging').copy()
        self.configs_by_host = OrderedDict()
        for host in sorted(self.config.get('hosts')):
            self.configs_by_host[host] = dict(self.config.items() + [('host', host)])
        self.alive_configs = [cfg for cfg in self.configs_by_host.values()]
        self.conn = None
        self.lock = threading.RLock()

    def start(self):
        self.liveness_thread = threading.Thread(target=self._liveness_check_loop)
        self.liveness_thread.daemon = True
        self.liveness_thread.start()
        logger.info('RabbitMQ load balancer started')

    def mark_down(self, host):
        self.lock.acquire()
        try:
            to_remove = self.configs_by_host[host]
            if to_remove in self.alive_configs:
                logger.warning('Marking RabbitMQ host as down: %s' % host)
                # To keep self.connect lock free, don't modify the self.alive list
                self.alive_configs = [cfg for cfg in self.alive_configs if cfg != to_remove]
        finally:
            self.lock.release()

    def mark_up(self, host):
        self.lock.acquire()
        try:
            cfg = self.configs_by_host[host]
            if cfg not in self.alive_configs:
                logger.warning('Marking RabbitMQ host as up: %s' % host)
                # To keep self.connect lock free, don't modify the self.alive list
                self.alive_configs = self.alive_configs + [cfg]
        finally:
            self.lock.release()

    def _connect(self, cfg=None):
        if not self.alive_configs:
            raise NoHostsAvailable()
        if not cfg:
            cfg = random.choice(self.alive_configs)

        heartbeat_interval = 10 + random.randint(0, 10)
        conn = pika.BlockingConnection(pika.ConnectionParameters(host=cfg['host'], heartbeat_interval=heartbeat_interval))
        return conn, cfg['host']

    def forward(self, func):
        """
        Will raise NoHostsAvailable if all brokers are down
        """
        if not self.conn:
            self.conn, self.host = self._connect()
        try:
            channel = self.conn.channel()
            exchange = self.config['exchange']
            channel.exchange_declare(exchange=exchange['name'], type=exchange['type'])
            func.foo(self.conn.channel(), exchange['name'])
        except (CouldNotDeliver, ConnectionClosed):
            logger.info('RabbitMQ host %s went away. Trying another one.' % self.host)
        except Exception as ex:
            logger.exception(ex)
            raise
        
    def check_liveness(self, con_cache=None):
        if con_cache is None:
            con_cache = {}

        # Assumes only one thread is doing this
        for cfg in self.configs_by_host.values():
            host = cfg['host']
            try:
                con = con_cache.get(host)
                if con is None:
                    con, _ = self._connect(cfg)
                    con_cache[host] = con
                try:
                    self._ping(con)
                    self.mark_up(host)
                finally:
                    pass
                    # con.close()
            except Exception as ex:
                print ex
                con_cache.pop(host, None)
                self.mark_down(host)

    def _ping(self, con):
        # WILL THIS FILL UP THE DISK?
        channel = con.channel()
        channel.queue_declare(queue='hello')
        channel.basic_publish(exchange='', routing_key='hello', body='Hello World!')

    def _liveness_check_loop(self):
        con_cache = {}
        while True:
            self.check_liveness(con_cache)
            # Some randomness is good if we start a bunch of Gunicorn workers at the same time, spreads MySQL load
            time.sleep(10 + random.randint(0, 10))


class RabbitMqClient(object):

    def __init__(self, routing_key, queue, rabbit_mq):
        self.routing_key = routing_key
        self.queue = queue
        self.rabbit_mq = rabbit_mq

    def publish(self, message):
        producer = RabbitMqProducer(message, self.routing_key)
        self.rabbit_mq.forward(producer)

    def consume(self, callback):
        consumer = RabbitMqConsumer(self.queue, self.routing_key, callback)
        self.rabbit_mq.forward(consumer)


class RabbitMqProducer(object):

    def __init__(self, message, routing_key):
        self.message = message
        self.routing_key = routing_key

    def foo(self, channel, exchange):
        PERSISTANT = 2
        channel.confirm_delivery()
        if not channel.basic_publish(
                exchange=exchange,
                routing_key=self.routing_key,
                body=self.message,
                properties=pika.BasicProperties(delivery_mode=PERSISTANT),
                mandatory=True):
            raise CouldNotDeliver('exchange=%s routing_key=%s message=%s' % (exchange, self.routing_key, str(self.message)))


class RabbitMqConsumer(object):

    def __init__(self, queue, routing_key, callback):
        self.queue = queue
        self.routing_key = routing_key
        self.callback = callback

    def _create_queue(self, channel):
        channel.queue_declare(queue=self.queue, durable=True, exclusive=False, auto_delete=False)

    def _bind_queue(self, channel, exchange):
        channel.queue_bind(exchange=exchange, queue=self.queue, routing_key=self.routing_key)

    def foo(self, channel, exchange):
        self._create_queue(channel)
        self._bind_queue(channel, exchange)
        ack_handler = AcknowledgedCallback(self.routing_key, self.callback, channel)
        channel.basic_consume(ack_handler.handle, queue=self.queue)
        channel.start_consuming()