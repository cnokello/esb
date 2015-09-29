"""CSVReader: Handles reading from a CSV file
   @author: okello
   @created: 12-aug-2015

"""
import logging
import glob
import json
import csv


class CSVReader(object):
    
    def __init__(self, cfg, msg_broker_services, msg_consumer_service):
        """Initializes required attributes.
           @params:
             file_path: Location of the CSV file to be read
             delimiter: Specifies the field delimiter
             read_mode: Specifies whether to read the whole
             file as a whole of incrementally.
        
        """
        self.logger = logging.getLogger(__name__)
        
        self.file_path = cfg.get("csv_file", "base_dir").strip()
        self.delimiter = cfg.get("csv_file", "delimiter").strip()
        self.read_mode = cfg.get("csv_file", "read_mode").strip()
        self.msg_producer_service = msg_broker_services.get("reader")
        
        # ## Load names and fields of CSV files to process
        files = dict(cfg.items("csv_fields"))
        
        self.file_fields = {}
        for filename, fields in files.iteritems():
            self.file_fields[filename] = [field_name \
                for field_name in fields.split("\n") \
                if len(field_name) > 0]
            
        self.files_to_process = self.file_fields.keys()
        
        self.logger.info("CSVReader Initialized; File Path: %s, Delimiter: %s, Read Mode: %s..." % \
            (self.file_path, self.delimiter, self.read_mode))
        
    def __del__(self):
        """Performs cleanup
        
        """
        self.logger.info("Destroyed!!!")
            
    def run(self):
        """Reads and processes only CSV files specified in the config file.
           A JSON string of fied_name: field_value is published to a message broker.
        
        """
        for current_file in glob.glob(self.file_path.rstrip("/") + "/*.csv"):
            file_to_process = (current_file.split("/")[-1]).rstrip(".csv")
            self.logger.info("File to process: %s" % file_to_process)
            if file_to_process in self.files_to_process:
                with open(current_file) as file:
                    for line in file:
                        if line and len(line.strip()) > 0:
                            csv_record = (csv.reader(
                                [line.strip()], delimiter=self.delimiter,
                                quotechar='"')).next()
                            record = dict(
                                zip(self.file_fields[file_to_process],
                                    csv_record))
                            file_record = {}
                            file_record["file"] = file_to_process
                            file_record["record"] = record
                            msg = json.dumps(file_record)
                            self.msg_producer_service.publish(msg)
                            self.logger.debug("READER ### Published: %s" % msg)