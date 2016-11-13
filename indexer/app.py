#!/usr/bin/env python3
import requests
import xml.etree.ElementTree
import exifread
import threading
import io
import urllib.request
import json
import logging
import pika
from pika import exceptions

SOURCE_URL = 'http://s3.amazonaws.com/waldo-recruiting'
KEY_TAG = '{http://s3.amazonaws.com/doc/2006-03-01/}Key'

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

# RabbitMQ connection
credentials = pika.PlainCredentials('guest', 'guest')
connection = pika.BlockingConnection(pika.ConnectionParameters(
    host='rabbitmq', port=5672, retry_delay=1, connection_attempts=10))
# Create a queue
channel = connection.channel()
channel.queue_declare(queue='exifs')

def get_image_keys():
    '''
    Return a list of keys for the images stored in SOURCE_URL.
    '''
    keys = list()
    data = requests.get(SOURCE_URL)
    elems = xml.etree.ElementTree.fromstring(data.text)
    for elem in elems.getiterator(tag=KEY_TAG):
        keys.append(elem.text)
    return keys


def index_image_exifs(keys=None):
    '''
    Concurrently get all image exif data for each image at the SOURCE_URL+key
    for key in keys. Finally upload that data to the index.
    '''
    if not keys:
        keys = get_image_keys()
    for key in keys:
        #curio.run(get_image_exif(key))
        thread = threading.Thread(target=get_image_exif, args=(key,))
        thread.start()

    # is elastic search ready?
    es_ready = False
    while not es_ready:
        es_health = requests.get("http://localhost:9200/_cluster/health")
        if es_health.json()["status"] in ["yellow", "green"]:
            es_ready = True
        else:
            time.sleep(5)

    # Elastic Search is ready
    # It would be nice to do this concurrently too, but
    # I received a "Slow down" message from elasticsearch
    # So we'll do it sequentially. You could increase the
    # ES throughput by sharding and using haproxy.
    channel.basic_consume(callback,
                          queue='exifs')
    channel.start_consuming()
    LOGGER.info("Creating indexes...")


def callback(ch, method, properties, body):
    '''Callback for a RabbitMQ consumer. Sends exif data to elasticsearch.'''
    exif = json.loads(body)
    res = requests.post("http://localhost:9200/exif/{}/1".format(exif.keys()[0]),
                        data=exif.values()[0])
    # acknowledge the message
    ch.basic_ack(delivery_tag = method.delivery_tag)
    LOGGER.info(" [x] Received %r" % body)


def get_image_exif(key):
    '''
    Get exif data for one image, identified by it's key and
    add the resulting key/value tuple to the queue.
    '''

    data = urllib.request.urlopen("{0}/{1}".format(SOURCE_URL, key))
    image = data.read()
    LOGGER.info('Downloading image: {}'.format(key))
    exif = get_exif(image)
    # Send the data to RabbitMQ
    channel = connection.channel()
    channel.basic_publish(exchange='',
                          routing_key='exifs',
                          body=json.dumps({key: exif}))
    channel.close()


def get_exif(image):
    '''
    Extract the exif data from a raw image.
    '''
    exif = exifread.process_file(io.BytesIO(image), details=False)
    ret = dict(zip(exif.keys(),
                   # exif values are ifd tag instances, which may
                   # need recursive treatment. Here we convert to
                   # strings and ignore that possibility.
                   # We also ignore string encoding errors.
               map(lambda x: str(x),
                   exif.values())))
    return json.dumps(ret)


if __name__ == "__main__":
    index_image_exifs()
