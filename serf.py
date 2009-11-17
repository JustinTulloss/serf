
class Serf(object):
    queues = {}
    def __init__(self, **config):
        self.config = config

    def enqueue(self, queue, data = {}):
        if not self.queues.has_key(queue):
            self.queues[queue] = Queue(queue, **config)
        self.queues[queue].push(data)
