from .worker import Consumer
from .arguments import Arguments


def start_worker(broker_ip, broker_port, username, password, incoming_queue, result_exchange, result_routing, result_queue, topic_type):
    worker_arguments = Arguments()
    worker_arguments.broker_ip = broker_ip
    worker_arguments.broker_port = broker_port
    worker_arguments.username = username
    worker_arguments.password = password
    worker_arguments.incoming_queue = incoming_queue
    worker_arguments.result_exchange = result_exchange
    worker_arguments.result_routing = result_routing
    worker_arguments.result_queue = result_queue
    worker_arguments.topic_type = topic_type

    consumer = Consumer(worker_arguments)
    consumer.consume()
