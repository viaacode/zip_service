import argparse
from .worker import Consumer
from .arguments import Arguments


def main():
    arguments = Arguments()

    parser = argparse.ArgumentParser()
    parser.add_argument('broker_ip', help='the ip adress/hostname of the rabbit cluster', type=str)
    parser.add_argument('incoming_queue', help='the incoming queue to bind to for incoming messages', type=str)
    parser.add_argument('result_exchange', help='the exchange to send the result to', type=str)
    parser.add_argument('result_routing', help='the routing key to use when posting the result', type=str)
    parser.add_argument('username', help='your rabbitMQ username', type=str)
    parser.add_argument('password', help='your rabbitMQ password', type=str)
    parser.add_argument('-p', '--broker_port', help='the broker port (defaults to: 5672)', type=int, default=5672)
    parser.add_argument('-q', '--result_queue', help='queue to be declared if none exists', type=str, default=None)
    parser.add_argument('-t', '--topic_type', help='the type of the exchange should it be created', type=str, default=None)
    parser.parse_args(namespace=arguments)

    try:
        print("Starting...use CTRL-C to exit.")
        consumer = Consumer(arguments)
        consumer.consume()
    except KeyboardInterrupt:
        print("Exited.")

if __name__ == "__main__":
    main()
