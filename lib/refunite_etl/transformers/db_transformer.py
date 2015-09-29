"""DB Transformer: Transforms messages for
   database insertion or update
   @author: okello
   @created: 15-aug-2015

"""
import json
import re
import logging
from sys import exc_info


class DBTransformer(object):
    
    def __init__(
        self, cfg, msg_producer_service, msg_consumer_service):
        self.logger = logging.getLogger(__name__ + "." + self.__class__.__name__)
        
        self.find_non_json = re.compile(r"^([^{]*).*")
        
        self.msg_producer_service = msg_producer_service
        self.msg_consumer_service = msg_consumer_service
        
        # ## Get file table mapping configurations
        file_table_cfg = dict(cfg.items("db_transformation"))
        
        self.table_info = {}
        table_info_parser = TableInfoParser()
        for file_name, table_cfg_info in file_table_cfg.iteritems():
            table_defs = table_cfg_info.split(";")
            
            tables = {}
            tables["tables"] = [] 
            table_number = 1
            for table_def in table_defs:
                tables["tables"].append(table_info_parser.parse(
                    table_def.strip()))
                
            self.table_info[file_name] = tables
            
    def process_msg(self, msg):
        msg_dict = json.loads(msg.replace(re.search(self.find_non_json, msg).group(1), ""))
        file_name = msg_dict["file"]
        record = msg_dict["record"]
        self.logger.debug("RECORD: %s" % record)
        
        table_data = self.table_info[file_name]
        tables = table_data["tables"]
        
        target_tables = []
        
        try:
            for table in tables:
                table_name = table["table_name"]
                fields = table["fields"]
            
                target_table = {}
                target_fields = []
            
                for field in fields:
                    target_field = {}
                    target_field["name"] = field["to_field"]
                    target_field["data_type"] = field["data_type"]
                    target_field["value"] = record[field["from_field"]]
                    target_fields.append(target_field)
                
                target_table[table_name] = target_fields
                target_tables.append(target_table)
            
            self.msg_producer_service.publish(json.dumps(target_tables))
            
        except Exception as e:
            self.logger.error("Error while constructing target DB table; Message: %s" % json.dumps(msg_dict), exc_info=True)
        
        self.logger.debug("TRANSFORMER ### Published Message: %s..." % json.dumps(target_tables))
              
    def run(self):
        self.logger.info("Running transformation...")
        self.logger.debug("Complete tables info: %s" % json.dumps(self.table_info))
        
        self.msg_consumer_service.consume(self.process_msg)
            
        
class TableInfoParser(object):
    
    def __init__(self):
        self.logger = logging.getLogger(__name__ + "." + self.__class__.__name__)
        self.logger.info("Parsing table information...")
        
    def parse(self, table_cfg_info):
        self.logger.info("Table Cfg Info: %s" % table_cfg_info)
        cfg_info = table_cfg_info.split("=")
        
        table_name = cfg_info[0].strip()
        self.logger.info("Table Name: %s" % table_name)
        field_info = cfg_info[1].strip()
        self.logger.debug("Fields: %s" % field_info)
        
        self.logger.debug("Table Name: %s, Field Info: %s" % (table_name, field_info)) 
        
        table_info = {}
        table_info["table_name"] = table_name
        table_info["fields"] = []
        field_infos = field_info.split("\n")
        for info in field_infos:
            field_details = info.split("%")
            data_type = field_details[0].strip()
            field_mapping = field_details[1].split(">")
            from_field = field_mapping[0].strip()
            to_field = field_mapping[1].strip()
            
            field  = {}
            field["data_type"] = data_type
            field["from_field"] = from_field
            field["to_field"] = to_field
            
            table_info["fields"].append(field)
            
        return table_info
        