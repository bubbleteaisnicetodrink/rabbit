import logging
from typing import Callable

import pika

LOGGER = logging.getLogger(__name__)


def get_job(job_func: Callable[[str], bool], queue: str, host: str, port: int, user: str, pwd: str) -> None:
    if user != '':
        credentials = pika.PlainCredentials(username=user, password=pwd)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host, port=port, credentials=credentials))
    else:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host, port=port))
    channel = connection.channel()
    channel.queue_declare(queue=queue, durable=True)
    LOGGER.info(" [*] Waiting for messages. To exit press CTRL+C")

    def callback(ch, method, properties, body):
        LOGGER.info(" [x] Received %r" % body)
        success = job_func(body)
        if success:
            LOGGER.info(" [x] Done")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            LOGGER.info(" [x] Failed")
            ch.basic_nack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue, on_message_callback=callback)
    channel.start_consuming()


def add_job(message: str, queue: str, host: str, port: int, user: str, pwd: str) -> None:
    if user != '':
        credentials = pika.PlainCredentials(username=user, password=pwd)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host, port=port, credentials=credentials))
    else:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host, port=port))
    channel = connection.channel()
    channel.queue_declare(queue=queue, durable=True)
    message = message
    channel.basic_publish(
        exchange='',
        routing_key=queue,
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ))
    print(" [x] Sent %r" % message)
    connection.close()
