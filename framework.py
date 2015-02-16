import pika
from pika.exceptions import AMQPError
import time
import random
from collections import OrderedDict
from threading import Condition
from select import error as SelectError
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


class ProduceError(Exception):
    pass


class RabbitMqConnection(object):
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
        self.started = Condition()

    def get(self):
        if not self.alive_configs:
            raise NoHostsAvailable()

        cfg = random.choice(self.alive_configs)
        return self.connect(cfg), cfg['host']

    def start(self):
        self.started.acquire()
        self.liveness_thread = threading.Thread(target=self._liveness_check_loop)
        self.liveness_thread.daemon = True
        self.liveness_thread.start()
        self.started.wait(timeout=10)
        self.started.release()
        logger.info('RabbitMQ load balancer started')

    def _mark_down(self, host):
        self.lock.acquire()
        try:
            to_remove = self.configs_by_host[host]
            if to_remove in self.alive_configs:
                logger.warning('Marking RabbitMQ host as down: %s' % host)
                # To keep self.connect lock free, don't modify the self.alive list
                self.alive_configs = [cfg for cfg in self.alive_configs if cfg != to_remove]
        finally:
            self.lock.release()

    def _mark_up(self, host):
        self.lock.acquire()
        try:
            cfg = self.configs_by_host[host]
            if cfg not in self.alive_configs:
                logger.warning('Marking RabbitMQ host as up: %s' % host)
                # To keep self.connect lock free, don't modify the self.alive list
                self.alive_configs = self.alive_configs + [cfg]
        finally:
            self.lock.release()

    def connect(self, cfg):
        heartbeat_interval = 10 + random.randint(0, 10)
        conn = pika.BlockingConnection(pika.ConnectionParameters(host=cfg['host'], heartbeat_interval=heartbeat_interval))
        return conn

    def _check_liveness(self, con_cache=None):
        if con_cache is None:
            con_cache = {}

        # Assumes only one thread is doing this
        for cfg in self.configs_by_host.values():
            host = cfg['host']
            try:
                con = con_cache.get(host)
                if con is None:
                    con = self.connect(cfg)
                    con_cache[host] = con
                try:
                    self._ping(con)
                    self._mark_up(host)
                finally:
                    pass
                    # con.close()
            except Exception as ex:
                con_cache.pop(host, None)
                self._mark_down(host)

    def _ping(self, con):
        channel = con.channel()
        channel.queue_declare(queue='hello')
        channel.basic_publish(exchange='', routing_key='hello', body='Hello World!')

    def _liveness_check_loop(self):
        con_cache = {}
        first_time = False
        while True:
            if first_time:
                self.started.acquire()
                self._check_liveness(con_cache)
                self.started.notify()
                self.started.release()
                first_time = False
            else:
                self._check_liveness(con_cache)

            # Some randomness is good if we start a bunch of Gunicorn workers at the same time
            time.sleep(5 + random.randint(0, 5))
            

class RabbitMq(object):
    
    def __init__(self, config, rabbit_mq_connection):
        self.config = config.get('messaging').copy()
        self.rabbit_mq_connection = rabbit_mq_connection
        self.rabbit_mq_connection.start()
        self.channel = None
        self.conn = None
        self.host = None

    def forward(self, caller):
        backoffs = [0] + [2**i for i in range(5)]
        for backoff in backoffs:
            time.sleep(backoff)
            try:
                if not self.conn:
                    self.conn, self.host = self.rabbit_mq_connection.get()
                    self.channel = self.conn.channel()
                exchange = self.config['exchange']
                self.channel.exchange_declare(exchange=exchange['name'], type=exchange['type'])
                return caller.provide(self.channel, exchange['name'])
            except (ProduceError, AMQPError, SelectError) as ex:
                if backoff == backoffs[-1]:
                    raise NoHostsAvailable('Tried to reach a new host %s times during %s seconds' % (len(backoffs), sum(backoffs)))
                logger.info('RabbitMQ host %s went away because of %s. Trying another one.' % (self.host, ex.__class__))
                self.conn = None


class RabbitMqClient(object):

    def __init__(self, routing_key, queue, rabbit_mq):
        self.routing_key = routing_key
        self.queue = queue
        self.rabbit_mq = rabbit_mq

    def publish(self, message):
        producer = Produce(message, self.routing_key)
        self.rabbit_mq.forward(producer)

    def consume(self, callback):
        consumer = Consume(self.queue, self.routing_key, callback)
        self.rabbit_mq.forward(consumer)


class Produce(object):

    def __init__(self, message, routing_key):
        self.message = message
        self.routing_key = routing_key

    def provide(self, channel, exchange):
        PERSISTANT = 2
        channel.confirm_delivery()
        if not channel.basic_publish(
                exchange=exchange,
                routing_key=self.routing_key,
                body=self.message,
                properties=pika.BasicProperties(delivery_mode=PERSISTANT),
                mandatory=True):
            raise ProduceError('exchange=%s routing_key=%s message=%s' % (exchange, self.routing_key, str(self.message)))


class Consume(object):

    def __init__(self, queue, routing_key, callback):
        self.queue = queue
        self.routing_key = routing_key
        self.callback = callback

    def _create_queue(self, channel):
        channel.queue_declare(queue=self.queue, durable=True, exclusive=False, auto_delete=False)

    def _bind_queue(self, channel, exchange):
        channel.queue_bind(exchange=exchange, queue=self.queue, routing_key=self.routing_key)

    def provide(self, channel, exchange):
        self._create_queue(channel)
        self._bind_queue(channel, exchange)
        ack_handler = AcknowledgedCallback(self.routing_key, self.callback, channel)
        channel.basic_consume(ack_handler.handle, queue=self.queue)
        channel.start_consuming()