import datetime
import logging
import os
import time
import urllib.parse

from json import loads,dumps
from os.path import join

from . import zipper
import pika
from pika import PlainCredentials
from pika import exceptions


LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)


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
        logging.info('%s function took %0.3f s' % (f.__name__, (time2 - time1)))
        return ret

    return wrap

class AsyncConsumer(object):

        #EXCHANGE = arguments.result_exchange
    #EXCHANGE_TYPE = 'topic'
    #QUEUE = 'text'
    #ROUTING_KEY = 'from.transcoder'

    def __init__(self, arguments):
        self.EXCHANGE = 'INCOMING_ZIPPER'
        self.EXCHANGE_TYPE = arguments.topic_type
        self.ROUTING_KEY = 'TO_ZIPPER'
        self.host=arguments.broker_ip
        self.port = arguments.broker_port
        self.broker_user=arguments.username
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self.error_routing = 'ERROR'
        self.error_queue = 'zipper_errors'
        self.vhost_plain = arguments.vhost
        self.vhost=urllib.parse.quote_plus(arguments.vhost)
        self.passwd=arguments.password
        self.queue = arguments.incoming_queue
        self.result_exchange = arguments.result_exchange
        self.result_routing = arguments.result_routing
        self.result_queue = arguments.result_queue
        self.QUEUE = arguments.incoming_queue
        self.topic_type = arguments.topic_type
        self._url = "amqp://%s:%s@%s:%s/%s?heartbeat_interval=0" % (arguments.username,self.passwd,
                                                                    arguments.broker_ip,arguments.broker_port,
                                                                    self.vhost)

        def __format__(self, *args, **kwargs):
            return super().__format__(*args, **kwargs)

    def connect(self):
        LOGGER.info('Connecting to %s', self._url)
        return pika.SelectConnection(pika.URLParameters(self._url),

                                     self.on_connection_open,

                                     stop_ioloop_on_close=False,
                                     )

    def on_connection_open(self, unused_connection):
        LOGGER.info('Connection opened')
        self.add_on_connection_close_callback()
        self.open_channel()

    def add_on_connection_close_callback(self):
        LOGGER.info('Adding connection close callback')
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param int reply_code: The server provided reply_code if given
        :param str reply_text: The server provided reply_text if given

        """
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            LOGGER.warning('Connection closed, reopening in 5 seconds: (%s) %s',
                           reply_code, reply_text)
            self._connection.add_timeout(5, self.reconnect)


    def reconnect(self):
        LOGGER.info('reconnect called')
        self._connection.ioloop.stop()
        self.publish_channel = self.publish_connection.channel()
        if not self._closing:
            # Create a new connection
            self._connection = self.connect()
            # There is now a new connection, needs a new ioloop to run
            self._connection.ioloop.start()

    def open_channel(self):
        LOGGER.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        LOGGER.info('Channel opened')
        self._channel = channel
        self._channel.basic_qos(prefetch_count=1)
        self._channel.confirm_delivery()
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def add_on_channel_close_callback(self):
        LOGGER.info('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed

        """
        LOGGER.warning('Channel %i was closed: (%s) %s',
                       channel, reply_code, reply_text)
        self._connection.close()

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        LOGGER.info('Declaring exchange %s', exchange_name)
        self._channel.exchange_declare(self.on_exchange_declareok,
                                       exchange_name,
                                       self.EXCHANGE_TYPE)

    def on_exchange_declareok(self, unused_frame):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

        """
        LOGGER.info('Exchange declared')
        self.setup_queue(self.QUEUE)

    def setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.

        :param str|unicode queue_name: The name of the queue to declare.

        """
        LOGGER.info('Declaring queue %s', queue_name)
        self._channel.queue_declare(self.on_queue_declareok, queue_name, durable=True)

    def on_queue_declareok(self, method_frame):

        LOGGER.info('Binding %s to %s with %s',
                    self.EXCHANGE, self.QUEUE, self.ROUTING_KEY)
        self._channel.queue_bind(self.on_bindok, self.QUEUE,
                                 self.EXCHANGE, self.ROUTING_KEY)

    def on_bindok(self, unused_frame):
        LOGGER.info('Queue bound')
        self.start_consuming()

    def start_consuming(self):
        LOGGER.info('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(self.on_message,
                                                         self.QUEUE)

    def add_on_cancel_callback(self):
        LOGGER.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        LOGGER.info('Consumer was cancelled remotely, shutting down: %r',
                    method_frame)
        if self._channel:
            self._channel.close()

    def on_message(self, unused_channel, basic_deliver, properties, body):
        LOGGER.info('Received message # %s from %s: %s',
                    basic_deliver.delivery_tag, properties.app_id, body)

        try:
            params = loads(body.decode("utf-8"))
            status = 'OK'
            details = 'Zipfile Created.'

            if validate_message(params):
                try:
                    root = params['source_path']
                    zipfilename = join(params['destination_path'], params['destination_file'])
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
                    self.publish_connection = pika.BlockingConnection(pika.ConnectionParameters(
                        host=self.host,
                        port=int(self.port),
                        virtual_host=self.vhost_plain,
                        heartbeat_interval=int(0),
                        retry_delay=3,
                        connection_attempts=60,
                        credentials=PlainCredentials(self.broker_user, self.passwd)
                    ))
                    self.publish_channel = self.publish_connection.channel()
                    channel=self.publish_channel
                    if self.result_queue is not None and self.topic_type is not None:
                        self.publish_channel.exchange_declare(exchange=self.result_exchange, type=self.topic_type)
                        self.publish_channel.queue_declare(queue=self.result_queue, durable=True)
                        self.publish_channel.queue_bind(queue=self.result_queue, exchange=self.result_exchange,
                                                        routing_key=self.result_routing)
                    channel.basic_publish(exchange=self.result_exchange, routing_key=self.result_routing,
                                          body=json_message)
                    logging.info("Message published to: " + self.result_exchange + "/" + self.result_routing)
                    self.acknowledge_message(basic_deliver.delivery_tag)
                    self.publish_connection.close()
                except:
                    logging.error('reconnecting to queue needed ')

                    self.publish_connection = pika.BlockingConnection(pika.ConnectionParameters(
                        host=self.host,
                        port=int(self.port),
                        virtual_host=self.vhost_plain,
                        heartbeat_interval=int(0),
                        retry_delay=3,
                        connection_attempts=60,
                        credentials=PlainCredentials(self.broker_user, self.passwd)
                    ))
                    self.publish_channel = self.publish_connection.channel()
                    channel=self.publish_channel
                    channel.basic_publish(exchange=self.result_exchange, routing_key=self.result_routing, body=json_message)
                    logging.info('sendmessage after reconnect: {}'.format(json_message))
                    self.acknowledge_message(basic_deliver.delivery_tag)
                    logging.info('stopping consumer, ioloop, and closing connection')
                    self.publish_connection.close()
                    self._connection.close()
                    self.reconnect()
                    self.run()

                    pass


            else:
                logging.error('Message invalid: {}'.format(params))
                try:
                    self.acknowledge_message(basic_deliver.delivery_tag)
                except:

                    pass

        except Exception as e:
            logging.error(str(e))
            logging.error('this was on message final error message')
            self.reconnect()
            self.run()
            pass

    def acknowledge_message(self, delivery_tag):
        LOGGER.info('Acknowledging message %s', delivery_tag)
        self._channel.basic_ack(delivery_tag)

    def stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.
        """
        if self._channel:
            LOGGER.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def on_cancelok(self, unused_frame):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame

        """
        LOGGER.info('RabbitMQ acknowledged the cancellation of the consumer')
        self.close_channel()

    def close_channel(self):
        """Call to close the channel with RabbitMQ cleanly by issuing the
        Channel.Close RPC command.

        """
        LOGGER.info('Closing the channel')
        self._channel.close()

    def run(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.

        """
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

        """
        LOGGER.info('Stopping')
        self._closing = True
        self.stop_consuming()
        self._connection.ioloop.start()
        LOGGER.info('Stopped')

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        LOGGER.info('Closing connection')
        self._connection.close()

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