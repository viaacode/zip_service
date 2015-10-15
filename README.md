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
    zipper [-h] [-p BROKER_PORT] [-q RESULT_QUEUE] [-t TOPIC_TYPE] broker_ip incoming_queue result_exchange result_routing username password
```

Information about every parameter can be consulted with:

```
    zipper -h
```

# Documentation

Any further documentation can be found on the [VIAA Confluence page](https://viaadocumentation.atlassian.net/wiki/display/SI/Zipper)