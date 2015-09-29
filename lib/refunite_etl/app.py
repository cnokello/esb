"""App: Starts all the different applications in separate threads
   @author: okello
   @created: 15-aug-2015

"""
import logging
import logging.config
from refunite_etl.cfg import Cfg
from refunite_etl.app_creator import AppCreator


class App(object):
    
    def __init__(self, cfg_file_path):
        """Start the various components. The order of startup must be:
           - Writers
           - Transformers
           - Parsers
           - Readers
        
        """
        self.configurator = Cfg()
        cfg = self.configurator.get_cfg(cfg_file_path)
        
        # ## Logging configuration and setup
        try:
            logging.config.fileConfig(cfg.get("logging", "cfg_file").strip())
        except Exception:
            raise Exception(
                "Error loading logging configuration. Please confirm that \
                logging configuration is correct")
        
        self.logger = logging.getLogger(__name__ + "." + self.__class__.__name__)
        
        # ## Get message broker producers
        self.brokers = get_message_broker_producers(cfg, self.configurator)
        
        # ## Writer apps
        writers = get_writer_cfg(cfg)
        writer_apps = []
        for consumer_cfg, writer_class in writers:
            writer_apps.append(AppCreator(cfg, self.brokers, consumer_cfg, writer_class))
        
        # ## Transformer apps
        transformers = get_transformer_cfg(cfg)
        transformer_apps = []
        for consumer_cfg, transformer_class in transformers:
            transformer_apps.append(AppCreator(cfg, self.brokers, consumer_cfg, transformer_class))
        
        # ## Parser apps
        parser_consumer_cfg, parser_class = get_parser_cfg(cfg)
        parser_app = AppCreator(cfg, self.brokers.get("parser"), parser_consumer_cfg, parser_class)
        
        # ## Reader apps
        reader_consumer_cfg, reader_classes = get_reader_cfg(cfg)
        reader_apps = []
        for reader_class in reader_classes:
            reader_apps.append(AppCreator(cfg, self.brokers, reader_consumer_cfg, reader_class))
        
        # ## Start all the apps
        for writer_app in writer_apps:
            writer_app.start()
            
        for transformer_app in transformer_apps:
            transformer_app.start()
        
        parser_app.start()
        
        for reader_app in reader_apps:
            reader_app.start()
        
    
def get_message_broker_producers(cfg, configurator):
    brokers = {}
    producer_class = configurator.get_class(
        cfg.get("global", "producer_class").strip())

    brokers["reader"]  = producer_class(
        cfg.get("global", "parser_url").strip(),
        cfg.get("global", "topic_parse").strip())
    brokers["parser"] = producer_class(
        cfg.get("global", "transformer_url").strip(),
        cfg.get("global", "topic_transform").strip())
    brokers["transformer"] = producer_class(
        cfg.get("global", "writer_url").strip(),
        cfg.get("global", "topic_write").strip())
    brokers["writer"] = producer_class(
        cfg.get("global", "summary_url").strip(),
        cfg.get("global", "topic_summary").strip())
    brokers["db"] = producer_class(
        cfg.get("global", "db_url").strip(),
        cfg.get("global", "topic_db").strip())
    
    return brokers
    
    
def get_parser_cfg(cfg):
    consumer_cfg = {}
    consumer_cfg["class"] = cfg.get("global", "consumer_class").strip()
    consumer_cfg["url"] = cfg.get("global", "parser_url").strip()
    consumer_cfg["topic"] = cfg.get("global", "topic_parse").strip()
        
    parser_class = cfg.get("csv_file", "parser_class").strip()
    
    return (consumer_cfg, parser_class)
    
    
def get_reader_cfg(cfg):
    consumer_cfg = {}
    consumer_cfg["class"] = cfg.get("global", "consumer_class").strip()
    consumer_cfg["url"] = cfg.get("global", "reader_url").strip()
    consumer_cfg["topic"] = cfg.get("global", "topic_read").strip()
    
    reader_classes = []    
    reader_classes.append(cfg.get("csv_file", "reader_class").strip())
    reader_classes.append(cfg.get("json_file", "reader_class").strip())
    
    
    return (consumer_cfg, reader_classes)


def get_transformer_cfg(cfg):
    transformers = []
    
    consumer_cfg = {}
    consumer_cfg["class"] = cfg.get("global", "consumer_class").strip()
    consumer_cfg["url"] = cfg.get("global", "transformer_url").strip()
    consumer_cfg["topic"] = cfg.get("global", "topic_transform").strip()
        
    db_transformer_class = cfg.get("transformers", "db_transformer").strip()
    transformers.append((consumer_cfg, db_transformer_class))
    
    # ## Agent info transformer
    consumer_cfg = {}
    consumer_cfg["class"] = cfg.get("global", "consumer_class").strip()
    consumer_cfg["url"] = cfg.get("global", "transformer_url").strip()
    consumer_cfg["topic"] = "agent_data"
        
    agent_transformer_class = cfg.get("transformers", "agent_transformer").strip()
    transformers.append((consumer_cfg, agent_transformer_class))
    
    return transformers


def get_writer_cfg(cfg):
    # ## CSV DB Writer
    writers = []
    consumer_cfg = {}
    consumer_cfg["class"] = cfg.get("global", "consumer_class").strip()
    consumer_cfg["url"] = cfg.get("global", "writer_url").strip()
    consumer_cfg["topic"] = cfg.get("global", "topic_write").strip()
    writer_class = cfg.get("csv_file", "writer_class").strip()
    
    writers.append((consumer_cfg, writer_class))
    
    # ## E-mail Writer
    consumer_cfg = {}
    consumer_cfg["class"] = cfg.get("global", "consumer_class").strip()
    consumer_cfg["url"] = cfg.get("global", "transformer_url").strip()
    consumer_cfg["topic"] = cfg.get("global", "topic_email").strip()
    writer_class = cfg.get("general_writers", "email_writer_class").strip()
    
    writers.append((consumer_cfg, writer_class))
    
    # ## SMS Writer
    consumer_cfg = {}
    consumer_cfg["class"] = cfg.get("global", "consumer_class").strip()
    consumer_cfg["url"] = cfg.get("global", "transformer_url").strip()
    consumer_cfg["topic"] = cfg.get("global", "topic_sms").strip()
    writer_class = cfg.get("general_writers", "sms_writer_class").strip()
    
    writers.append((consumer_cfg, writer_class))
    
    # ## Notification writer
    consumer_cfg = {}
    consumer_cfg["class"] = cfg.get("global", "consumer_class").strip()
    consumer_cfg["url"] = cfg.get("global", "transformer_url").strip()
    consumer_cfg["topic"] = cfg.get("global", "topic_notification").strip()
    writer_class = cfg.get("general_writers", "notification_writer_class").strip()
    
    writers.append((consumer_cfg, writer_class))
    
    return writers