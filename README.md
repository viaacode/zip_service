# zip_service

This is a worker that will listen to a queue from RabbitMQ, zip the file specified in the incoming message and will post the result to the given result exchange

# Installation

Simply check out this repository and run:

```
    [sudo] python3 setup.py install [--record files.txt]
```

# Usage

You can run the worker by executing the following command:

```
    zipper [-h] [--broker_ip BROKER_IP]
                  [--vhost /]
                  [--incoming_queue INCOMING_QUEUE]
                  [--result_exchange RESULT_EXCHANGE]
                  [--result_routing RESULT_ROUTING] [--username USERNAME]
                  [--password PASSWORD] [--broker_port BROKER_PORT]
                  [--result_queue RESULT_QUEUE] [--topic_type TOPIC_TYPE]
```

Information about every parameter can be consulted with:

```
    zipper -h
```

The worker can be configured with a zipper.conf file that has be located in /etc/viaa-workers. If it doesn't exists the command line arguments will be used. You can use command line arguments to overrule property values.

```
[DEFAULT]
BROKER_IP=192.168.56.101                #The ip of the RabbitMQ Broker
BROKER_PORT=5672                        #The port of the RabbitMQ Broker, comment out if not needed. This will default to 5672
INCOMING_QUEUE=incoming_queue           #The name of the queue the worker will listen to
RESULT_EXCHANGE=result_exchange         #The name of result exchange where the worker will publish its result messages to
RESULT_ROUTING=result_routing           #The name of the routing key to be used to publish messages
RESULT_QUEUE=result_queue               #The name of the result queue if this doesn't exist yet. Comment out if not needed
TOPIC_TYPE=direct                       #The name of the topic type for the result queue. Comment out if not needed
USERNAME=guest                          #The username to access the RabbitMQ broker
PASSWORD=guest                          #The passwrod to access the RabbitMQ broker
VHOST=/                                 #The virtual host to use
```

# Documentation

Any further documentation can be found on the [VIAA Confluence page](https://viaadocumentation.atlassian.net/wiki/display/SI/Zipper)
