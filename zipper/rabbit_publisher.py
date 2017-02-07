import pika
import logging
from pika.credentials import PlainCredentials
from pika import exceptions

__author__ = 'viaa'


def send_message(host, port, vhost, username, password, exchange, routing_key, queue, topic_type, message):
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=host,
        port=port,
        virtual_host=vhost,
        connection_attempts=6,
        retry_delay=2,
        heartbeat_interval=6,
        credentials=PlainCredentials(username, password)
    ))

    channel = connection.channel(channel_number=2)
    if queue is not None and topic_type is not None:
        channel.exchange_declare(exchange=exchange, type=topic_type)
        channel.queue_declare(queue=queue, durable=True)
        channel.queue_bind(queue=queue, exchange=exchange, routing_key=routing_key)
    try:
        channel.basic_publish(exchange=exchange, routing_key=routing_key, body=message,
                              properties=pika.BasicProperties(delivery_mode=2))
        connection.close(reply_text='closed cause done and published')

    except Exception as e:
        print('connection err:', str(e))
        channel = connection.channel(channel_number=3)
        try:
            channel.basic_publish(exchange=exchange, routing_key=routing_key, body=message)
            connection.close(reply_text='closed in exception handling')
        except Exception as e:
            print('reconnected publish failedtoo :s O_°')

    logging.info("Message published to: " + exchange + "/" + routing_key)
