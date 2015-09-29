"""AppCreator: This creates the different applications that
   have been configured for an instance of the system
   @author: okello
   @created: 12-aug-2015

"""
import logging
import threading
from refunite_etl.cfg import Cfg 


class AppCreator(threading.Thread):
    
    def __init__(self, cfg, broker_producer, consumer_cfg, class_cfg):
        self.logger = logging.getLogger(__name__ + "." + self.__class__.__name__)
        threading.Thread.__init__(self)
        
        self.logger.debug("Creating app; Consumer Config: %s, Class: %s..." % (consumer_cfg, class_cfg))
        
        # ## Load configurations
        self.configurator = Cfg()
        
        self.cfg = cfg
        self.broker_producer = broker_producer
        self.consumer_cfg = consumer_cfg
        self.class_cfg = class_cfg
        
    def run(self, ):
        # ## Configure Message Consumer service
        self.logger.info("Getting Message Consumer Service...")
        msg_consumer_class = self.configurator.get_class(
            self.consumer_cfg["class"])
        msg_consumer_service = msg_consumer_class(
            self.consumer_cfg["url"],
            self.consumer_cfg["topic"])
        
        # ## Configure data parsers
        self.logger.info("Configuring data parsers...")
        csv_class = self.configurator.get_class(
            self.class_cfg)
        csv_class(
            self.cfg, self.broker_producer,
            msg_consumer_service).run()
