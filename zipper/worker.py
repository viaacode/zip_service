import datetime
import logging
from threading import Timer

import pika
import os
import time
from pika.credentials import PlainCredentials
from json import loads, dumps
from .rabbit_publisher import send_message
from os.path import join, relpath
from . import zipper
from pika import exceptions


def validate_message(params):
    mandatory_keys = [
        'correlation_id',
        'source_server',
        'source_path',
        'destination_server',
        'destination_path',
        'destination_file'
    ]

    message_valid = True


    for key in mandatory_keys:
        if key not in params:
            message_valid = False
            logging.error('{} is missing from received message'.format(key))

    return message_valid


def timing(f):
    def wrap(*args):
        time1 = time.time()
        ret = f(*args)
        time2 = time.time()
        logging.info('%s function took %0.3f s' % (f.__name__, (time2-time1)))
        return ret
    return wrap



class Consumer:

    def __init__(self, arguments):
        self.host = arguments.broker_ip
        self.port = arguments.broker_port
        self.vhost = arguments.vhost
        self.username = arguments.username
        self.password = arguments.password
        self.queue = arguments.incoming_queue
        self.result_exchange = arguments.result_exchange
        self.result_routing = arguments.result_routing
        self.result_queue = arguments.result_queue
        self.topic_type = arguments.topic_type
        self.file_permission = 0o770
        self.user = ''
        self.group = ''





    def consume(self):

        connection = pika.BlockingConnection(pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                virtual_host=self.vhost,
                heartbeat_interval=0,
                retry_delay=2,
                socket_timeout=6000,
                connection_attempts=10,
                credentials=PlainCredentials(self.username, self.password)
        ))




        channel = connection.channel(1)
        channel.basic_qos(prefetch_count=1)
        channel.queue_declare(queue=self.queue, durable=True)
        channel.basic_consume(self.callback, self.queue)
        # def check():
        #     time.sleep(10)
        #     logging.info('start check')
        #     if connection.is_closed:
        #
        #
        #         logging.info('connection is closed!')
        #         logging.info('Larry is no more')
        #         connection.channel(2)
        #
        #
        # Timer(10, check(),)
        channel.start_consuming()













    def callback(self, ch, method, properties, body):
        try:
            params = loads(body.decode("utf-8"))
            status = 'OK'
            details = 'Zipfile Created.'

            if validate_message(params):
                try:
                    root = params['source_path']
                    zipfilename = join (params['destination_path'], params['destination_file'])
                    self.supermakedirs(params['destination_path'])
                    logging.info('zipping .. be patient')

                    @timing
                    def zipping():

                        zipper.zip_dir(root, zipfilename, **params)
                        logging.info('zipping finished !')



                    zipping()


                except Exception as e:
                    logging.error(str(e))
                    status = 'NOK'
                    details = str(e)

                message = {
                    "correlation_id": params["correlation_id"],
                    "status": status,
                    "description": details,
                    "destination_server": params["destination_server"],
                    "destination_path": params["destination_path"],
                    "destination_file": params["destination_file"],
                    "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }

                json_message = dumps(message)
                logging.info(json_message)
                try:
                    send_message(
                            self.host,
                            self.port,
                            self.vhost,
                            self.username,
                            self.password,
                            self.result_exchange,
                            self.result_routing,
                            self.result_queue,
                            self.topic_type,
                            json_message
                    )
                except pika.exceptions.ConnectionClosed or pika.exceptions.AMQPError as e:
                    print ('err:',str(e), 'in worker.py')

                    send_message(
                        self.host,
                        self.port,
                        self.vhost,
                        self.username,
                        self.password,
                        self.result_exchange,
                        self.result_routing,
                        self.result_queue,
                        self.topic_type,
                        json_message
                    )
                    connection = pika.BlockingConnection(pika.ConnectionParameters(
                        host=self.host,
                        port=self.port,
                        virtual_host=self.vhost,
                        credentials=PlainCredentials(self.username, self.password)
                    ))
                    channel = connection.channel()
                    channel.basic_qos(prefetch_count=1)
                    channel.queue_declare(queue=self.queue, durable=True)
                    channel.basic_consume(self.callback, self.queue)
                    channel.start_consuming()

            else:
                logging.error('Message invalid: {}'.format(params))
                ch.basic_ack(delivery_tag=method.delivery_tag)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logging.error(str(e))

    def supermakedirs(self, path):
        try:
            if not path or os.path.exists(path):
                stat_info = os.stat(path)
                uid = stat_info.st_uid
                gid = stat_info.st_gid
                self.user = uid
                self.group = gid
                logging.debug('Found: {} - {} - {}'.format(self.user, self.group, path))
                # Break recursion
                return []
            (head, tail) = os.path.split(path)
            res = self.supermakedirs(head)
            os.mkdir(path)
            os.chmod(path, self.file_permission)
            os.chown(path, self.user, self.group)
            logging.debug('Created: {} - {} - {}'.format(self.user, self.group, path))
            res += [path]
            return res
        except OSError as e:
            if e.errno == 17:
                logging.debug('Directory existed when creating. Ignoring')
                res += [path]
                return res
            raise
