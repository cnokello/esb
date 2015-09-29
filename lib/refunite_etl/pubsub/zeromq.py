"""ZeroMQ: Implements communication with ZeroMQ Pub/Sub system
   @author: okello
   @created: 14-aug-2015

"""
from __future__ import print_function
import zmq
import logging


class ZeroMQProducer(object):
    
    def __init__(self, url, topic):
        self.logger = logging.getLogger(__name__ + "." + self.__class__.__name__)
        ctx = zmq.Context()
        self.url = url
        self.topic = topic
        self.socket = ctx.socket(zmq.XPUB)
        
        self.socket.bind(url)    
        
    def publish(self, msg, topic=None):
        to_topic = topic
        if not to_topic:
            to_topic = self.topic
        
        self.logger.debug("Published Message: %s %s to %s" % (to_topic, msg, self.url))
        self.socket.send("%s %s" % (to_topic, msg))
        

class ZeroMQConsumer(object):
    
    def __init__(self, url, topic):
        ctx = zmq.Context()
        self.socket = ctx.socket(zmq.SUB)
        self.socket.setsockopt(zmq.SUBSCRIBE, topic)
        self.socket.connect(url)
        
    def consume(self, callback_fn):
        while True:
            msg = self.socket.recv()
            callback_fn(msg)