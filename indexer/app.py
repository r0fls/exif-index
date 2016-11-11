#!/usr/bin/env python
import requests
import xml.etree.ElementTree
import exifread
import threading
from cStringIO import StringIO
import urllib2
import Queue
import json
import logging

SOURCE_URL = 'http://s3.amazonaws.com/waldo-recruiting'
KEY_TAG = '{http://s3.amazonaws.com/doc/2006-03-01/}Key'

logging.basicConfig()


def get_image_keys():
    '''
    Return a list of keys for the images stored in SOURCE_URL.
    '''
    keys = list()
    # lazy way of dealing with rate limiting
    done = False
    while not done:
        try:
            data = requests.get(SOURCE_URL)
            done = True
        except requests.exceptions.ConnectionError:
            time.sleep(5)
    elems = xml.etree.ElementTree.fromstring(data.text)
    for elem in elems.getiterator(tag=KEY_TAG):
        keys.append(elem.text)
    return keys


def index_image_exifs(keys=None):
    '''
    Concurrently get all image exif data for each image at the SOURCE_URL+key
    for key in keys. Finally upload that data to the index.
    '''
    queue = Queue.Queue()
    if not keys:
        keys = get_image_keys()
    for key in keys:
        thread = threading.Thread(target=get_image_exif, args=(key,queue))
        thread.start()

    # Wait for all of the threads to finish
    print("Waiting for images to download...")
    thread.join()
    print("Images finished downloading.")

    # is elastic search ready? Really we should use a messaging queue.
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
    # So we'll do it sequentially.
    while not queue.empty():
        exif = queue.get()
        res = requests.post("http://localhost:9200/exif/{}/1".format(exif[0]),
                            data=exif[1])
        print(res.text)
    print("Finished indexing.")

def get_image_exif(key, queue):
    '''
    Get exif data for one image, identified by it's key.
    '''

    data = urllib2.urlopen("{0}/{1}".format(SOURCE_URL, key))
    image = data.read()
    # This should log instead of print
    print('Downloading image: {}'.format(key))
    exif = get_exif(image)
    queue.put((key, exif))


def get_exif(image):
    '''
    Extract the exif data from a raw image.
    '''
    exif = exifread.process_file(StringIO(image))
    ret = dict(zip(exif.keys(),
                   # exif values are ifd tag instances, which may
                   # need recursive treatment. Here we convert to
                   # strings and ignore that possibility.
                   # We also ignore string encoding errors.
               map(lambda x: unicode(str(x), errors='ignore'),
                   exif.values())))
    return json.dumps(ret)


if __name__ == "__main__":
    index_image_exifs(get_image_keys())
