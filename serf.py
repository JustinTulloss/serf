from amqplib import client_0_8 as amqp

try:
    import json
except ImportError:
    import simplejson as json

class Serf(object):
    def __init__(self,
            port = 5672,
            host = "localhost",
            user = "guest",
            password = "guest"):

        self._connection = amqp.Connection(
            host = "%s:%d" % (host, port),
            userid = user,
            password = password,
            virtual_host = "/",
            insist = False)
        self._channel = self._connection.channel()

    def enqueue(self, queue, data = {}):
        self._channel.queue_declare(
            queue = queue,
            durable = True,
            exclusive = False)
