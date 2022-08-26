#----------------------------------------------------------------------------
# simple-publisher
# Simple Solace direct message Publisher
#
# nram, Aug 25, 2022

import sys, os
import signal
import argparse
import pprint
import yaml
import inspect
import time, datetime
import random
import traceback
import string

# Import Solace Python  API modules from the solace package
from solace.messaging.messaging_service import MessagingService, ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener, RetryStrategy, ServiceEvent
from solace.messaging.resources.topic import Topic
from solace.messaging.publisher.direct_message_publisher import PublishFailureListener
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.receiver.message_receiver import MessageHandler, InboundMessage


# Globals
Cfg = {}    # cfg dict
pp = pprint.PrettyPrinter(indent=4)

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

class TopicPublisher () :
    ''' Solace topic publisher implementation '''

    def __init__(self, topic, message):
        ''' Constructor. '''
        print ( f'{T()}: Init TopicPublisher ...')
        self.name = 'TopicPublisher'
        cfg_b = Cfg['broker']
        self.sol = SolaceBroker (cfg_b['url'], 
                                cfg_b['vpn'],
                                cfg_b['client']['username'], 
                                cfg_b['client']['password'],
                                self.name)
        self.publish(topic, message)
        #self.stop()

    def publish (self, topic, message):
        print ( f'{T()}: {self.name} publish ...')
        try:
            self.sol.connect()
            self.sol.topic_publisher()
            print (f'{T()}: {self.name} publishing to {topic}')
            self.sol.publish(topic, message)
            print (f'{T()}: published')
        except Exception as e:
            print(f'Unexpected error in TopicPublisher\n{e} ({sys.exc_info()[0]}')
            print(traceback.format_exc())
            self.stop() # IllegalStateError is attempting to publish after stop is called.
        finally:
            self.stop()


    def stop(self):
        print(f'{T()}: {self.name} Stopping ...')
        self.sol.direct_publisher.terminate()
        self.sol.close()
        #os.kill(os.getpid(), signal.SIGINT)

class YamlHandler():
    """ YAML handling functions """

    def __init__(self):
        print()

    def read_file(self, file):
        """ read yaml file and return data """
        with open(file, "r") as fp:
            data = yaml.safe_load(fp)
        return data

def Prep(config_file):
    print (f"{T()}: Reading user config {config_file}")
    yaml_h = YamlHandler()
    cfg = yaml_h.read_file(config_file)
    print ('CONFIG'); pp.pprint (cfg)
    return cfg

def main(argv):
    global Cfg, SysCfg 

    p = argparse.ArgumentParser()
    p.add_argument('--configfile', dest="config_file", required=False, 
            default='config/solace.yml', help='config file')
    r = p.parse_args()

    Cfg = Prep(r.config_file)

    print (f'{T()}: Starting publisher')
    pub = TopicPublisher('test/from/ubuntu/5', 'Hello from ubuntu')
    print (f'{T()}: Exiting ...')

if __name__ == "__main__":
   main(sys.argv[1:])
