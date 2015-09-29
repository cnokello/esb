"""NotificationWriter: Sends notifications using SMS and E-mail channels
   
   @author: okello
   @created: 26-aug-2015

"""
import logging
import re
import json
import time, datetime


class MessageTemplate(object):
    
    def __init__(self, line):
        """Creates a message template object from a line representing a message template record 
        
        """
        self.logger = logging.getLogger(__name__ + "." + self.__class__.__name__)
        self.logger.info("Creating a message template object...")
        
        record = line.strip().split("|", -1)
        self.notification_type = record[0].strip()
        self.channel = record[1].strip()
        self.language = record[2].strip()
        self.subject = record[3].strip()
        self.message = record[4].strip()
        self.composed_message = None
        self.created = datetime.datetime.utcfromtimestamp(
            time.time()).strftime("%Y-%m-%d %H:%M:%S")
        
    def get_key(self):
        """Creates and returns the key to use for this message template 
        
        """
        return self.notification_type.upper() + "." + \
            self.channel.upper() + "." + self.language.upper()
            
    def get_composed_msg(self, template_params):
        """Creates a complete composed message by replacing placeholders with the exact data 
        
        """
        self.logger.debug("Composing message with params: %s..." % template_params)
        fields = re.findall('\[\[.+?\]\]', self.message, re.DOTALL)
        msg = self.message
        for field in fields:
            clean_field = field.replace("[[", "").replace("]]", "")
            field_value = template_params.get(clean_field, None)
            self.logger.info(
                ("Message Template; Template Params: %s, Field Name: %s, Value: %s" % 
                (template_params, field, field_value)))
                
            if(field_value is None):
                return False
            msg = msg.replace(field, str(field_value))
            
        self.composed_message = msg
        self.logger.info("Composed Message: %s" % msg)
        return msg
    
    def to_db(self):
        """Creates and returns database schema plus data to be submitted to the DB writer 
        
        """
        fields = []
        
        notification_type = {}
        notification_type["data_type"] = "string"
        notification_type["name"] = "notification_type"
        notification_type["value"] = self.notification_type
        fields.append(notification_type)
        
        channel = {}
        channel["data_type"] = "string"
        channel["name"] = "channel"
        channel["value"] = self.channel
        fields.append(channel)
        
        language = {}
        language["data_type"] = "string"
        language["name"] = "language"
        language["value"] = self.language
        fields.append(language)
        
        subject = {}
        subject["data_type"] = "string"
        subject["name"] = "subject"
        subject["value"] = self.subject
        fields.append(subject)
        
        message = {}
        message["data_type"] = "string"
        message["name"] = "message"
        message["value"] = self.composed_message
        fields.append(message)
        
        created = {}
        created["data_type"] = "string"
        created["name"] = "created"
        created["value"] = self.created
        fields.append(created)
        
        table = {}
        table["notification"] = fields
        
        tables = []
        tables.append(table)
        
        self.logger.debug("DB Tables: %s" % json.dumps(tables))
        
        return json.dumps(tables)
            
    def to_str(self):
        """Creates and returns a string representation of the message template object 
        
        """
        return (
            "Notification Type: %s\nChannel: %s\nLanguage: %s\nSubject: %s\nMessage: %s\n" %\
            (self.notification_type, self.channel, self.language, self.subject, self.message))
        
    
class Notification(object):
    """Notification model class
    
    """
    
    def __init__(self, msg):
        self.logger = logging.getLogger(__name__ + "." + self.__class__.__name__)
        self.logger.info("Processing notification request: %s..." % msg)
        self.language = msg.get("language").strip().upper() 
        
        params = msg.get("params")
        self.channels = params.get("channels")
        self.notification_type = params.get("type")
        self.recipient_id = params.get("recipient_id")
        
    def compose_email(self, subject, msg, recipient, sender):
        self.logger.debug(
            "Composing E-mail Message with params; Subject: %s, \
            Message: %s, Recipient: %s, Sender: %s..." %\
            (subject, msg, recipient, sender))
        email_msg = {}
        email_msg["sender"] = sender
        email_msg["recipient"] = recipient
        email_msg["subject"] = subject
        email_msg["message"] = msg
        
        return email_msg
    
    def compose_sms(self, msg, recipient):
        self.logger.debug(
            "Composing SMS message with the params; Message: %s,\
            Recipient: %s..." % (msg, recipient))
        
        sms_msg = {}
        sms_msg["recipient"] = recipient
        sms_msg["message"] = msg
        
        return sms_msg
    
    def send(self, msg_templates, parser_broker_service, transformer_broker_service,  topic_email, topic_sms):
        self.logger.debug("Sending out notification...")
        
        for channel_cfg in self.channels:
            channel = channel_cfg.get("channel")
            email_phone = channel_cfg.get("key")
            template_params = channel_cfg.get("template_params")
            key = self.notification_type.upper() + "." + channel.upper() + "." + self.language.upper()
            
            self.logger.debug(
                "Sending notification; Channel: %s, Email_Phone: %s, Template Params: %s, Key: %s..." %\
                 (channel, email_phone, template_params, key))
            
            msg_template = msg_templates.get(key)
            if msg_template:
                self.logger.debug("Matched Message Template: %s" % msg_template.to_str())
            
                if channel.strip().upper() == "WEB":
                    composed_email_msg = msg_template.get_composed_msg(template_params)
                    composed_email = self.compose_email(msg_template.subject,
                        composed_email_msg, email_phone,
                        "on@refunite.org")
                    
                    if composed_email_msg and len(composed_email_msg.strip()) > 0:
                        parser_broker_service.publish(
                            json.dumps(composed_email), topic_email)
                    self.logger.info(
                        "Published notification message to topic: %s, message: %s" %\
                        (topic_email, composed_email))
                
                elif channel.strip().upper() == "SMS" or channel.strip().upper() == "USSD":
                    composed_sms_msg = msg_template.get_composed_msg(template_params)
                    composed_sms = self.compose_sms(
                        composed_sms_msg, email_phone)
                    
                    if composed_sms_msg and len(composed_sms_msg.strip()) > 0:
                        parser_broker_service.publish(json.dumps(composed_sms), topic_sms)
                
                    self.logger.debug("SMS and USSD Composed Message: %s" % composed_sms)
                
                    self.logger.info("Published notification message to topic: %s, message: %s" %\
                        (topic_sms, composed_sms))
                
                transformer_broker_service.publish(msg_template.to_db())
            

class NotificationWriter(object):
    """Processes notification requests
    
    """
    
    def __init__(self, cfg, msg_broker_services, msg_consumer_service):
        self.logger = logging.getLogger(__name__ + "." + self.__class__.__name__)
        self.logger.info("Starting NotificationWriter...")
        
        self.find_non_json = re.compile(r"^([^(\[|{)?]*).*")
        self.cfg = cfg
        self.msg_broker_services = msg_broker_services
        self.msg_consumer_service = msg_consumer_service
        self.msg_templates = self.load_notification_templates(
            cfg.get("notifications", "message_templates").strip())
    
    def load_notification_templates(self, file_path):
        msg_templates = {}
        with open(file_path, 'rb') as file:
            for line in file:
                msg_template = MessageTemplate(line)
                msg_templates[msg_template.get_key()] = msg_template
                
        self.logger.debug("Message Templates: %s" % msg_templates)
        return msg_templates
        
    def process_msg(self, msg):
        self.logger.debug("Processing message: %s..." % msg)
        notification_request = json.loads(msg.replace(re.search(
            self.find_non_json, msg).group(1), ""))
        
        
        parser_broker_service = self.msg_broker_services.get("parser")
        transformer_broker_service = self.msg_broker_services.get("transformer")
        Notification(notification_request).send(
            self.msg_templates, parser_broker_service, transformer_broker_service, self.cfg.get(
            "global", "topic_email").strip(), self.cfg.get("global", "topic_sms").strip())
        
    def run(self):
        self.msg_consumer_service.consume(self.process_msg)
