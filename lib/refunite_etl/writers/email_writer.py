"""EmailWriter: Sends e-mails
   The e-mail to be sent must be JSON encoded and have the following keys:
   subject
   sender
   message
   recipient
   
   @author: okello
   @created: 26-aug-2015

"""
import logging
import json
import re
import time, datetime
import sendgrid


class Email(object):
    
    def __init__(self, msg):
        self.logger = logging.getLogger(__name__ + "." + self.__class__.__name__)
        self.logger.debug("Creating e-mail message: %s" % msg)
        self.sender = msg.get("sender")
        self.subject = msg.get("subject")
        self.recipient = msg.get("recipient")
        self.message = msg.get("message")
        self.created = datetime.datetime.utcfromtimestamp(
            time.time()).strftime("%Y-%m-%d %H:%M:%S")
        self.status_code = None
        
    def send(self, api):
        self.logger.info("Sending e-mail...")
        email_msg = sendgrid.Mail()
        email_msg.set_subject(self.subject)
        email_msg.set_html(self.message)
        email_msg.set_text(self.message)
        email_msg.set_from(self.sender)
        email_msg.set_replyto(self.sender)
        email_msg.add_to(self.recipient)
        
        response = None, None
        try:
            response = api.send(email_msg)
        except Exception:
            self.logger.error("Could not send E-mail", exc_info=True)
            
        return response
    
    def to_db(self):
        fields = []
        
        sender = {}
        sender["data_type"] = "string"
        sender["name"] = "sender"
        sender["value"] = self.sender
        fields.append(sender)
        
        subject = {}
        subject["data_type"] = "string"
        subject["name"] = "subject"
        subject["value"] = self.subject
        fields.append(subject)
        
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
        
        status_code = {}
        status_code["data_type"] = "string"
        status_code["name"] = "status_code"
        status_code["value"] = self.status_code
        fields.append(status_code)
        
        table = {}
        table["email_notification"] = fields
        
        tables = []
        tables.append(table)
        
        self.logger.debug("E-mail Notification Tables: %s" % tables)
        
        return json.dumps(tables)

class EmailSender(object):
    """Sends e-mails
    
    """
    def __init__(self, cfg):
        self.logger = logging.getLogger(
            __name__ + "." + self.__class__.__name__)
        self.logger.info("Starting e-mail sender...") 
        self.email_api = sendgrid.SendGridClient(
            cfg.get("email_api", "username"),
            cfg.get("email_api", "password"))
        
    def send(self, msg, transformer_broker_service):
        email = Email(msg)
        response = email.send(self.email_api)
        
        transformer_broker_service.publish(email.to_db())
        
        return response
    

class EmailWriter(object):
    
    def __init__(self, cfg, msg_broker_services, msg_consumer_service):
        self.logger = logging.getLogger(__name__ + "." + self.__class__.__name__)
        self.find_non_json = re.compile(r"^([^(\[|{)?]*).*")
        self.cfg = cfg
        self.msg_broker_services = msg_broker_services
        self.msg_consumer_service = msg_consumer_service
        self.email_sender = EmailSender(cfg)
        
    def process_msg(self, msg):
        self.logger.debug("Processing message: %s" % msg)
        request_msg = json.loads(msg.replace(re.search(
            self.find_non_json, msg).group(1), ""))
        request_msg["sender"] = self.cfg.get("email_api", "reply_to").strip()

        transformer_broker_service = self.msg_broker_services.get("transformer")
        response_code, response_msg = self.email_sender.send(
            request_msg, transformer_broker_service)
        
        self.logger.debug(
            "Response to send e-mail request: %s %s" %
            (response_code, response_msg))
        
    def run(self):
        self.logger.info("Running EmailWriter...")
        self.msg_consumer_service.consume(self.process_msg)