# AWS lambda function
#   REST POST and SMF Publish to Solace on receiving an event.
#
# Requires: 
#    pysol-layer
#
# Input/Payload:
#   {
#       "key": "...",
#       "text": "...",
#       "rawPath": "/publish|/post"
#   } 
#
# nram, Solace PSG
# Jun 25, 2022


import json
import uuid
import requests
from requests.auth import HTTPBasicAuth
import datetime
import sys, traceback

# Import Solace Python  API modules from the solace package
from solace.messaging.messaging_service import MessagingService, ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener, RetryStrategy, ServiceEvent
from solace.messaging.resources.topic import Topic
from solace.messaging.publisher.direct_message_publisher import PublishFailureListener
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.receiver.message_receiver import MessageHandler, InboundMessage


# Solace Access Info
SolaceCfg = {'rest-url': 'http://solace.messaging.solace.cloud:9000',
            'smf-url': 'tcp://solace.messaging.solace.cloud:55555',
            'vpn': 'test-vpn',
            'username': 'test-user',
            'password': 'test123'}
    
GET_PATH='/get'
POST_PATH='/post'       # REST POST to Solace
PUBLISH_PATH='/publish' # SMF Publish to Solace

#--------------------------------------------------------------------------
# Get timestamp
#
class T:
    ''' return current timestamp '''
    def __str__(self):
        return f'{datetime.datetime.now()}'
    
#----------------------------------------------------------------------------
# SolaceBroker Class
#
class SolaceBroker:

    ''' implements solace broker connection handling '''

    #----------------------------------------------------------------------------
    # Inner classes for message, event and error handling
    #
    class MessageHandlerImpl(MessageHandler):
        ''' async message handler callback '''

        def __init__ (self, _name):
            self.name = _name

    class ServiceEventHandler(ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener):
        ''' solace event handlers '''

        def on_reconnected(self, e: ServiceEvent):
            print("\non_reconnected")
            print(f"Error cause: {e.get_cause()}")
            print(f"Message: {e.get_message()}")

        def on_reconnecting(self, e: "ServiceEvent"):
            print("\non_reconnecting")
            print(f"Error cause: {e.get_cause()}")
            print(f"Message: {e.get_message()}")

        def on_service_interrupted(self, e: "ServiceEvent"):
            print("\non_service_interrupted")
            print(f"Error cause: {e.get_cause()}")
            print(f"Message: {e.get_message()}")

    class PublisherErrorHandling(PublishFailureListener):
        ''' solace event handler '''

        def on_failed_publish(self, e: 'FailedPublishEvent'):
            print("on_failed_publish")


    def __init__(self, _smfurl, _vpn, _clientusername, _clientpasswd,
                       _name = "default", _verbose = 0):
        self.broker_props = {
            "solace.messaging.transport.host": _smfurl,
            "solace.messaging.service.vpn-name": _vpn,
            "solace.messaging.authentication.scheme.basic.username": _clientusername,
            "solace.messaging.authentication.scheme.basic.password": _clientpasswd
        }
        self.name = _name
        #print (f'broker_props: {self.broker_props}')

    def connect (self):
        print (f'{T()}: {self.name} Connecting to {self.broker_props["solace.messaging.transport.host"]} ({self.broker_props["solace.messaging.service.vpn-name"]})')
        self.messaging_service = MessagingService.builder().from_properties(self.broker_props)\
                    .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(20,3))\
                    .build()
        # Event Handeling for the messaging service
        self.service_handler = self.ServiceEventHandler()
        self.messaging_service.add_reconnection_listener(self.service_handler)
        self.messaging_service.add_reconnection_attempt_listener(self.service_handler)
        self.messaging_service.add_service_interruption_listener(self.service_handler)

        # Blocking connect
        self.messaging_service.connect()
        if not self.messaging_service.is_connected:
            raise Exception (f'Messaging Service not connected')

    def topic_publisher(self):
        print (f'{T()}: {self.name} Starting topic publisher')
        # Create a direct message publisher and start it
        self.direct_publisher = self.messaging_service.create_direct_message_publisher_builder().build()
        self.direct_publisher.set_publish_failure_listener(self.PublisherErrorHandling())

        # Blocking Start
        self.direct_publisher.start()
        if not self.direct_publisher.is_ready():
            raise Exception (f'Topic Publisher not ready.')
        return self.direct_publisher

    def publish(self, _topicname, _payload):
        try:
            outbound_msg_builder = self.messaging_service.message_builder() \
                    .with_application_message_id("sample_id") \
                    .with_property("application", "samples") \
                    .with_property("language", "Python")

            topic = Topic.of(_topicname)
            # Direct publish the message with dynamic headers and payload
            outbound_msg = outbound_msg_builder.build(_payload)
            self.direct_publisher.publish(destination=topic, message=outbound_msg)
            print (f'{T()}: [{self.name} ] -> {_topicname} ({len(_payload)} bytes)')


        except Exception as e:
            print(f'Unexpected error in SolaceBroker\n{e} ({sys.exc_info()[0]}')
            raise e

    def close(self):
        print('Disconnecting Messaging Service')
        self.messaging_service.disconnect()

#----------------------------------------------------------------
# Direct SMF Topic Publisher
#
class TopicPublisher () :
    ''' Solace topic publisher implementation '''

    def __init__(self):
        ''' Constructor. '''
        print ( f'{T()}: Init TopicPublisher ...')
        self.name = 'TopicPublisher'
        self.sol = SolaceBroker (SolaceCfg['smf-url'],
                                SolaceCfg['vpn'],
                                SolaceCfg['username'],
                                SolaceCfg['password'],
                                self.name)
        self.sol.connect()


    def publish (self, topic, message):
        print ( f'{T()}: {self.name} publish ...')
        try:
            self.sol.topic_publisher()
            print (f'{T()}: {self.name} publishing to {topic}')
            self.sol.publish(topic, message)
        except Exception as e:
            print(f'Unexpected error in TopicPublisher\n{e} ({sys.exc_info()[0]}')
            print(traceback.format_exc())
            self.stop() # IllegalStateError is attempting to publish after stop is called.
        #finally:
        #    self.stop()

    def close(self):
        print(f'{T()}: {self.name} Stopping ...')
        self.sol.direct_publisher.terminate()
        self.sol.close()
        #os.kill(os.getpid(), signal.SIGINT)

#-----------------------------------------------------------------------------
# SMF Publish to Solace
#
def publish_to_solace(topic, payload):
    print ( f'{T()}: Init TopicPublisher ...')
    pub = TopicPublisher()
    pub.publish (topic, payload)
    pub.close()
   
#-----------------------------------------------------------------------------
# REST POST to Solace
#
def post_to_solace(topic, payload):

    solace_rest_url = "{}/TOPIC/{}".format(SolaceCfg['rest-url'], topic)
    hdrs = {"Content-Type": "application/json; charset=utf-8"}
    auth_hdr = HTTPBasicAuth(SolaceCfg['username'], SolaceCfg['password'])
    print ('Connecting to {} as {} (Topic: {})'.format(solace_rest_url, SolaceCfg['username'], topic))
    response = requests.post(solace_rest_url, 
                headers=hdrs,
                auth=auth_hdr, 
                json=payload)
    print ('Response:', response)
    print ('Response status code', response.status_code)
    return response.status_code
        
#------------------------------------------------------------------------------
# lambda handler - entry point
#
def lambda_handler(event, context):
    print ('Got an event')
    print (event)

    # GET
    if event['rawPath'] == GET_PATH:
        print ("Received Get Request")
        key = event['queryStringParameters']['key']
        print (f"Query transactionId {key}")
        resp = {}
        resp['status'] = 200
        #resp['type'] = event['rawPath']
        resp['key']= key
        resp['text'] = 'hello from lambda' # some stuff

        return {'statusCode': 200,
                'body': json.dumps(resp)}

    # REST POST Event to Solace
    elif event['rawPath'] == POST_PATH:
        print ("Received Post Request")
        print ("Got key: {} and text: {}".format(event['key'], event['text']))
        
        # post the event to Solace
        out_event = event.copy()
        out_event['timestamp'] = f'{T()}'
        post_to_solace('test/lambda/post/102', out_event)
        
        resp = {}
        resp['status'] = 200
        resp['description'] = 'REST POST to Solace'
        return {'statusCode': 200,
                'response': json.dumps(resp)}


    # SMF Publish to Solace with Python API
    elif event['rawPath'] == PUBLISH_PATH:
        print ("Received Publish Request")
        print ("Got key: {} and text: {}".format(event['key'], event['text']))
        
        out_event = event.copy()
        out_event['timestamp'] = f'{T()}'

        # publish the event to Solace
        publish_to_solace('test/lambda/pubish/202', out_event)
        
        resp = {}
        resp['status'] = 200
        resp['description'] = 'SMF Publish to Solace'
        return {'statusCode': 200,
                'response': json.dumps(resp)}
                