"""CSVParser: Parses CSV lines
   @author: okello
   @created: 12-aug-2015

"""
import logging
import json


class CSVParser(object):
    
    def __init__(self, cfg, msg_producer_service, msg_consumer_service):
        self.logger = logging.getLogger(__name__)
        
        self.cfg = cfg
        self.msg_producer_service = msg_producer_service
        self.msg_consumer_service = msg_consumer_service
        
        self.logger.info("CSV Parser initialized...")
        
    def process_msg(self, msg):
        msg_to_print = json.loads(
            msg.lstrip(
            self.cfg.get("global", "topic_parse")).strip())
        
        self.logger.debug("PARSER ### Received: %s" % json.dumps(msg_to_print))
        self.msg_producer_service.publish(msg)
        
        

    def run(self):
        self.msg_consumer_service.consume(self.process_msg)