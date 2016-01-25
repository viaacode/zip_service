import datetime
import logging
import pika
from pika.credentials import PlainCredentials
from json import loads, dumps
from .rabbit_publisher import send_message
from . import zipper


class Consumer:
    def __init__(self, arguments):
        self.host = arguments.broker_ip
        self.port = arguments.broker_port
        self.username = arguments.username
        self.password = arguments.password
        self.queue = arguments.incoming_queue
        self.result_exchange = arguments.result_exchange
        self.result_routing = arguments.result_routing
        self.result_queue = arguments.result_queue
        self.topic_type = arguments.topic_type

    def consume(self):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                credentials=PlainCredentials(self.username, self.password)
            ))

            channel = connection.channel()
            channel.queue_declare(queue=self.queue, durable=True)
            channel.basic_consume(self.callback, self.queue)
            channel.start_consuming()
        except Exception as e:
            logging.error(str(e))

    def callback(self, ch, method, properties, body):
        params = loads(body.decode("utf-8"))
        status = 'OK'
        details = 'Zipfile Created.'

        try:
            zipper.zip_dir(params['source_path'], params['destination_path'], params['destination_file'])
        except Exception as e:
            status = 'NOK'
            details = str(e)

        message = {
            "correlation_id": params["correlation_id"],
            "status": status,
            "description": details,
            "destination_server": params["destination_server"],
            "destination_path": params["destination_path"],
            "destination_file": params["destination_file"],
            "timestamp": datetime.datetime.now()
        }

        json_message = dumps(message)
        logging.info(json_message)

        send_message(
            self.host,
            self.port,
            self.username,
            self.password,
            self.result_exchange,
            self.result_routing,
            self.result_queue,
            self.topic_type,
            json_message
        )

        ch.basic_ack(delivery_tag=method.delivery_tag)
