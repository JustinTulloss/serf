"""
Copyright (c) 2009 Harmonize, Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

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
        """
        Creates a connection to a queue on a given amqp server. If the queue
        doesn't exist, it creates it.
        """

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

    def put(self, data):
        """
        Puts the passed data on the queue. Must be JSON serializable.
        """
        msg = amqp.Message(json.dumps(data))
        self._channel.basic_publish(msg, exchange = self.name)

    def get(self):
        """
        This is a blocking "get". Will return a message that is pushed
        onto this particular queue.
        """
        self._waiting = True
        self._consumeTag = self._channel.basic_consume(
            queue = self.name,
            callback = self._recv_callback)
        while self._waiting:
            self._channel.wait()
        self._channel.basic_cancel(self._consumeTag)
        return self._message

    def _recv_callback(self, msg):
        self._message = json.loads(msg.body)
        self._waiting = False

if __name__ == '__main__':
    """
    A sanity check
    """
    q = Queue('test')
    q.put({'message': 'this is a trial message'})
    print q.get()
