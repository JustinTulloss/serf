from amqplib import client_0_8 as amqp

try:
    import json
except ImportError:
    import simplejson as json

class Queue(object):
    def __init__(self, name,
            port = 5672,
            host = "localhost",
            user = "guest",
            password = "guest"):

        self.name = name

        self._connection = amqp.Connection(
            host = "%s:%d" % (host, port),
            userid = user,
            password = password,
            virtual_host = "/",
            insist = False)
        self._channel = self._connection.channel()

        self._channel.queue_declare(
            queue = name,
            durable = True,
            exclusive = False)
        self._channel.exchange_declare(
            name,
            'fanout',
            durable = True,
            auto_delete = False)
        self._channel.queue_bind(name, name)

    def __del__(self):
        if self._channel:
            self._channel.close()
        if self._connection:
            self._connection.close()

    def push(self, data):
        msg = amqp.Message(json.dumps(data))
        self._channel.basic_publish(msg, exchange = self.name)

if __name__ == '__main__':
    q = Queue('test')
    q.push({'message': 'this is a trial message'})