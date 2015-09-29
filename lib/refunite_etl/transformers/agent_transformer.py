"""Agent Transformer: Transforms agent informations
   @author: okello
   @created: 15-aug-2015

"""
import logging
import csv
import json


class AgentTransformer(object):
    
    def __init__(self, cfg, msg_broker_services, msg_consumer_service):
        self.logger = logging.getLogger(__name__ + "." + self.__class__.__name__)
        self.msg_producer_service = msg_broker_services.get("transformer")
        self.msg_consumer_service = msg_consumer_service    
        
    def process_msg(self, msg):
        msg = msg.replace("agent_data", "")
        self.logger.debug("Processing message: %s..." % msg.strip())
        agent = csv.reader([msg.strip()], delimiter=",", quotechar='"')
        agent_csv = agent.next()
        self.logger.debug("CSV Record: %s" % agent_csv)
        
        # ## Create database structure
        fields = []
        
        # ## Rollout field
        rollout = {}
        rollout["data_type"] = "string"
        rollout["name"] = "rollout"
        rollout["value"] = agent_csv[0]
        fields.append(rollout)
        
        # ## Iteration field
        iteration = {}
        iteration["data_type"] = "string"
        iteration["name"] = "iteration"
        iteration["value"] = agent_csv[1]
        fields.append(iteration)
        
        # ## Sub-Iteration
        sub_iteration = {}
        sub_iteration["data_type"] = "string"
        sub_iteration["name"] = "sub_iteration"
        sub_iteration["value"] = agent_csv[2]
        fields.append(sub_iteration)
        
        # ## Agent Name
        agent_name = {}
        agent_name["data_type"] = "string"
        agent_name["name"] = "agent_name"
        agent_name["value"] = agent_csv[3]
        fields.append(agent_name)
        
        # ## Number of Registrations
        number_of_registrations = {}
        number_of_registrations["data_type"] = "string"
        number_of_registrations["name"] = "number_of_registrations"
        number_of_registrations["value"] = agent_csv[4]
        fields.append(number_of_registrations)
        
        # ## Number of Quality Registrations
        number_of_quality_registrations = {}
        number_of_quality_registrations["data_type"] = "string"
        number_of_quality_registrations["name"] = "number_of_quality_registrations"
        number_of_quality_registrations["value"] = agent_csv[5]
        fields.append(number_of_quality_registrations)
        
        # ## Number of Missing Persons
        number_of_missing_persons = {}
        number_of_missing_persons["data_type"] = "string"
        number_of_missing_persons["name"] = "number_of_missing_persons"
        number_of_missing_persons["value"] = agent_csv[6]
        fields.append(number_of_missing_persons)
        
        # ## Pay Per Registration
        pay_per_registration = {}
        pay_per_registration["data_type"] = "string"
        pay_per_registration["name"] = "pay_per_registration"
        pay_per_registration["value"] = agent_csv[7]
        fields.append(pay_per_registration)
        
        # ## Bonus
        bonus = {}
        bonus["data_type"] = "string"
        bonus["name"] = "bonus"
        bonus["value"] = agent_csv[8]
        fields.append(bonus)
        
        # ## Lead Agent Commission
        lead_agent_commission = {}
        lead_agent_commission["data_type"] = "string"
        lead_agent_commission["name"] = "lead_agent_commission"
        lead_agent_commission["value"] = agent_csv[9]
        fields.append(lead_agent_commission)
        
        table = {}
        table["agent_data"] = fields
        
        tables = []
        tables.append(table)
        
        if number_of_registrations["value"].isdigit():
            self.msg_producer_service.publish(json.dumps(tables))
            self.logger.debug("Published: %s" % tables)
        
    def run(self):
        self.logger.info("Beginning message consumption...")
        self.msg_consumer_service.consume(self.process_msg)