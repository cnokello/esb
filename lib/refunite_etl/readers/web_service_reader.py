"""WebServiceReader: Listens for web service requests
   @author: okello
   @created: 25-aug-2015

"""
import logging
import json
from flask \
    import Flask, make_response, jsonify, request, abort
from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
    

class Response(object):
    
    def __init__(self):
        self.logger = logging.getLogger(
            __name__ + "." + self.__class__.__name__)
        
    def get(self, success, data):
        if success:
            return(make_response(jsonify(
                {'code': 200, "message": "Request successful", 'data': data})))
        else:
            return(make_response(jsonify(
                {'code': 800, "message": "Request failed", 'data': data})))
    

app = Flask(__name__)
response = Response()
logger = logging.getLogger(__name__) 
broker_producer = None
topic_email = None
topic_sms = None
topic_notification = None
topic_agent_data = None


@app.route("/test")
def test():
    logger.info("Handling test request...")
    return response.get(True, "Test completed")


@app.route("/etl", methods=["POST"])
def etl_process():
    logger.info("Handling a read operation...")
    request_data = request.get_data()
    logger.debug("Request Data: %s" % request_data)
    return response.get(True, "Read request handled")


@app.route("/email", methods=["POST"])
def send_email():
    logger.info("Handling a send e-mail request...")
    request_data = request.get_data()
    logger.debug("Publishing send e-mail request: %s" % request_data)
    
    broker_producer.publish(request_data, topic_email)
    logger.info("Send e-mail request published successfully to topic: %s..." % topic_email)
    return response.get(True, "Send e-mail request received successfully.")


@app.route("/sms", methods=["POST"])
def send_sms():
    logger.info("Handling a send SMS request...")
    request_data = request.get_data()
    logger.debug("Publishing send SMS request: %s" % request_data)
    
    broker_producer.publish(request_data, topic_sms)
    logger.info("Send e-mail request published successfully to topic: %s..." % topic_sms)
    return response.get(True, "Send e-mail request received successfully.")


@app.route("/notifications", methods=["POST"])
def send_notification():
    logger.info("Handling a create notification request...")
    request_data = request.get_data()
    logger.debug("Publishing create notification request: %s" % request_data)
    
    broker_producer.publish(request_data, topic_notification)
    logger.info("Create notification request published successfully to topic: %s..." % topic_notification)
    return response.get(True, "Create notification request received successfully.")


@app.route("/agent-data", methods=["POST"])
def agent_data():
    logger.info("Handling agent data...")
    agent_data = request.files["filedata"].read()
    
    for line in agent_data.split("\n"):
        broker_producer.publish(line, "agent_data")
    
    return response.get(True, "Process agent data request received successfully.")


class WebServiceReader(object):
    
    def __init__(self, cfg, msg_broker_services, msg_consumer_service):
        self.logger = logging.getLogger(__name__ + "." + self.__class__.__name__)
        self.logger.info("Starting web service reader server...")
        if_port = (cfg.get("servers", "web_service_reader_server").strip()).split(":")
        self.interface = if_port[0]
        self.port = if_port[1]
        self.msg_consumer_service = msg_consumer_service
        global broker_producer 
        global topic_email
        global topic_sms
        global topic_notification
        broker_producer = msg_broker_services.get("parser")
        topic_email = cfg.get("global", "topic_email").strip()
        topic_sms = cfg.get("global", "topic_sms").strip()
        topic_notification = cfg.get("global", "topic_notification").strip()
        
    def process(self):
        self.logger.ingo("Processing web service request...")
        
    def run(self):
        http_server = HTTPServer(WSGIContainer(app))
        http_server.listen(self.port)
        IOLoop.instance().start()
        