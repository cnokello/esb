"""DBWriter: Creates and synchronizes schema of
   database table(s) based on received info
   Inserts or updates a DB record as needed.
   
   @author: okello
   @created: 15-aug-2015

"""
import logging
import json
import re
from sqlalchemy import Table, MetaData, create_engine, Column, \
    String, Integer, TIMESTAMP


class DB(object):
    """Defines database connection
    
    """
    
    def __init__(self, cfg):
        self.logger = logging.getLogger(__name__ + "." + self.__class__.__name__)
        self.db = create_engine(cfg.get("db", "connect_string").strip())
        
    def get_connection(self):
        return self.db.connect()
        
        
class Schema(object):
    """Creates database schema
    
    """
    
    def __init__(self, cfg):
        self.logger = logging.getLogger(__name__ + "." + self.__class__.__name__)
        self.db = DB(cfg)
        self.tables = {}
        
    def get_data_type(self, type_str):
        if type_str.strip() == "string":
            return String(2000)
        elif type_str.strip() == "long":
            # return Integer
            return String(2000)
        elif type_str.strip() == "date":
            # return TIMESTAMP(timezone=True)
            return String(2000)
        else:
            return String(2000)
        
        
    def create(self, table_cfgs):
        self.logger.info("Creating DB schema...")
        with self.db.get_connection() as conn:
            conn.execute("SET search_path TO public")
            
            metadata = MetaData()
            for table_cfg in table_cfgs:
                for table_name, fields in table_cfg.iteritems():
                    
                    if not table_name in self.tables: 
                        table = Table(table_name, metadata, extend_existing=True)
                    
                        columns = []
                        for field in fields:
                            table = Table(
                                table_name,
                                metadata,
                                Column(field["name"], self.get_data_type(field["data_type"])),
                                extend_existing=True)
                    
                        metadata.create_all(conn)
                        self.tables[table_name] = table    
                        self.logger.info("WRITER ### Table Name: %s, Fields: %s" % (table_name, fields))
                    
                    # ## Now, insert data
                    table = self.tables.get(table_name)
                    data = {}
                    for field in fields:
                        data[field["name"]] = field["value"]
                    q = table.insert().values(data)        
                    conn.execute(q)
        

class DBWriter(object):
    """Processes database writing request including
       creating/updating underlying schema and inserting data
    
    """
    
    def __init__(self, cfg, msg_broker_services, msg_consumer_service):
        self.logger = logging.getLogger(__name__ + "." + self.__class__.__name__)
        self.find_non_json = re.compile(r"^([^(\[|{)?]*).*")
        
        self.cfg = cfg
        self.schema = Schema(self.cfg)
        self.msg_broker_services = msg_broker_services
        self.msg_consumer_service = msg_consumer_service
        
    def process_msg(self, msg):
        msg_dict = json.loads(msg.replace(re.search(self.find_non_json, msg).group(1), ""))
        self.schema.create(msg_dict)
        
        self.logger.info("WRITER ### Schema created: %s" % json.dumps(msg_dict))
        
    def run(self):
        self.msg_consumer_service.consume(self.process_msg)
    