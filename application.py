import time
import logging

logger = logging.getLogger(__name__)


class Producer(object):

    def __init__(self, id, n, client):
        self.id = id
        self.n = n
        self.client = client
        self.total_sent = 0

    def run(self):
        print 'Started producer %s' % self.id
        for i in xrange(self.n):
            try:
                self.client.publish('%s:%s' % (self.id, i))
            except KeyboardInterrupt:
                return
            self.total_sent += 1
            print 'Producer %s\tsent "%s:%s"\ttotal_sent %s' % (self.id, self.id, i, self.total_sent)


class Consumer(object):

    def __init__(self, id, sleep, client):
        self.id = id
        self.sleep = sleep
        self.client = client
        self.total_received = 0

    def run(self):
        print 'Started consumer %s' % self.id
        try:
            self.client.consume(self.callback)
        except KeyboardInterrupt:
            return

    def callback(self, message, subject=None):
        self.total_received += 1
        print 'Consumer %s\treceived "%s"\ttopic %s\ttotal_received %s' % (self.id, message, subject, self.total_received)
        time.sleep(self.sleep)