import argparse
from .worker import Consumer
from .arguments import Arguments
from configparser import ConfigParser
from os.path import exists
from sys import exit


def main():
    arguments = Arguments()

    parser = argparse.ArgumentParser()
    parser.add_argument('--broker_ip', help='the ip adress/hostname of the rabbit cluster', type=str)
    parser.add_argument('--incoming_queue', help='the incoming queue to bind to for incoming messages', type=str)
    parser.add_argument('--result_exchange', help='the exchange to send the result to', type=str)
    parser.add_argument('--result_routing', help='the routing key to use when posting the result', type=str)
    parser.add_argument('--username', help='your rabbitMQ username', type=str)
    parser.add_argument('--password', help='your rabbitMQ password', type=str)
    parser.add_argument('--broker_port', help='the broker port (defaults to: 5672)', type=int)
    parser.add_argument('--result_queue', help='queue to be declared if none exists', type=str)
    parser.add_argument('--topic_type', help='the type of the exchange should it be created', type=str)
    parser.parse_args(namespace=arguments)

    conf_path = '/etc/viaa-workers/zipper.conf'

    print("Initializing...")
    if exists(conf_path):
        parse_config(arguments, conf_path)
    else:
        print("WARNING: No config file found, using command line arguments...")

    check_arguments(arguments)

    try:
        print("Starting...use CTRL-C to exit.")
        consumer = Consumer(arguments)
        consumer.consume()
    except KeyboardInterrupt:
        print("Exited.")


def parse_config(arguments, conf_path):
    parser = ConfigParser()
    parser.read(conf_path)
    config = parser['DEFAULT']

    if arguments.broker_ip is None:
        arguments.broker_ip = config['BROKER_IP']

    if arguments.incoming_queue is None:
        arguments.incoming_queue = config['INCOMING_QUEUE']

    if arguments.result_exchange is None:
        arguments.result_exchange = config['RESULT_EXCHANGE']

    if arguments.result_routing is None:
        arguments.result_routing = config['RESULT_ROUTING']

    if arguments.username is None:
        arguments.username = config['USERNAME']

    if arguments.password is None:
        arguments.password = config['PASSWORD']

    if arguments.broker_port is None:
        try:
            arguments.broker_port = config['BROKER_PORT']
        except KeyError:
            arguments.broker_port = 5672

    if arguments.result_queue is None:
        try:
            arguments.result_queue = config['RESULT_QUEUE']
        except KeyError:
            print("WARNING: No result queue specified")

    if arguments.topic_type is None:
        try:
            arguments.topic_type = config['TOPIC_TYPE']
        except KeyError:
            print("WARNING: No topic type specified")


def check_arguments(arguments):
    if arguments.broker_ip is None:
        close("ERROR: No broker ip specified")

    if arguments.incoming_queue is None:
        close("ERROR: No incoming queue specified")

    if arguments.result_exchange is None:
        close("ERROR: No result exchange specified")

    if arguments.result_routing is None:
        close("ERROR: No result routing specified")

    if arguments.username is None:
        close("ERROR: No username specified")

    if arguments.password is None:
        close("ERROR: No password specified")


def close(message):
    print(message)
    exit(1)

if __name__ == "__main__":
    main()
