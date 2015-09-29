"""SMSWriter: Sends SMS
   The SMS to be sent must be JSON encoded and have the following keys:
       - message
       - recipient
   
   @author: okello
   @created: 26-aug-2015

"""
import logging
import re
import json
import time, datetime
import requests
from requests.auth import HTTPDigestAuth


class SMS(object):
    
    def __init__(self, msg):
        self.logger = logging.getLogger(__name__ + "." + self.__class__.__name__)
        
        self.recipient = msg.get("recipient")
        self.message = msg.get("message")
        self.created = datetime.datetime.utcfromtimestamp(
            time.time()).strftime('%Y-%m-%d %H:%M:%S')
        self.status = "SENT"
        
    def to_sms(self):
        sms_msg = {}
        sms_msg["to"] = self.recipient
        sms_msg["message"] = self.message
        
        return sms_msg
    
    def to_str(self):
        return (
            "Recipient: %s, Message: %s, Created: %s, Status: %s" %\
            (self.recipient, self.message, self.created, self.status))
    
    def to_db(self):
        fields = []
        
        recipient = {}
        recipient["data_type"] = "string"
        recipient["name"] = "recipient"
        recipient["value"] = self.recipient
        fields.append(recipient)
        
        message = {}
        message["data_type"] = "string"
        message["name"] = "message"
        message["value"] = self.message
        fields.append(message)
        
        created = {}
        created["data_type"] = "string"
        created["name"] = "created"
        created["value"] = self.created
        fields.append(created)
        
        status = {}
        status["data_type"] = "string"
        status["name"] = "status"
        status["value"] = self.status
        fields.append(status)
        
        table = {}
        table["sms_notification"] = fields
        
        tables = []
        tables.append(table)
        
        self.logger.debug("SMS Notification Tables: %s" % json.dumps(tables))
        
        return json.dumps(tables)
    
    
class SMSSender(object):
    
    def __init__(self, cfg):
        self.logger = logging.getLogger(__name__ + "." + self.__class__.__name__)
        self.logger.info("Starting SMS sender...")
        self.api_url = cfg.get("sms_api", "url").strip()
        self.api_key = cfg.get("sms_api", "key").strip()
        self.api_secret = cfg.get("sms_api", "secret").strip()
        self.method = cfg.get("sms_api", "method").strip()
        self.headers = {'Content-type': 'application/json',
                   'Accept': 'application/json'}
        
    def send(self, sms, transformer_broker_service):
        self.logger.info("Sending SMS; Method: %s, Params: %s..." % (self.method, sms.to_str()))
        
        response = None
        try:
            response = requests.request(
                    self.method, self.api_url, data=json.dumps(sms.to_sms()),
                    headers=self.headers, auth=HTTPDigestAuth(
                        self.api_key, self.api_secret))
        except Exception:
            self.logger.error("Could not send SMS", exc_info=True)
            sms.status= "NOT SENT"
            
        self.logger.debug("Response from SMS API: %s" % response)
        transformer_broker_service.publish(sms.to_db())
        
        
        return response
        

class SMSWriter(object):
    
    def __init__(self, cfg, msg_broker_services, msg_consumer_service):
        self.logger = logging.getLogger(__name__ + "." + self.__class__.__name__)
        self.logger.info("Starting SMS writer...")
        
        self.find_non_json = re.compile(r"^([^(\[|{)?]*).*")
        self.msg_broker_services = msg_broker_services
        self.msg_consumer_service = msg_consumer_service
        self.sms_sender = SMSSender(cfg)
        
    def process_msg(self, msg):
        self.logger.debug("Processing message: %s..." % msg)
        
        request_msg = json.loads(msg.replace(re.search(
            self.find_non_json, msg).group(1), ""))
        
        transformer_broker_service = self.msg_broker_services.get("transformer")
        response = self.sms_sender.send(SMS(request_msg), transformer_broker_service)
        self.logger.debug("Response from SMS API: %s" % response)
        
    def run(self):
        self.msg_consumer_service.consume(self.process_msg)