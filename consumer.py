import sys
import cv2
import numpy as np
from confluent_kafka import KafkaException, KafkaError, Consumer
import sounddevice as sd

conf = {'bootstrap.servers': "localhost:9092",
        'group.id': "foo",
        'auto.offset.reset': 'earliest'}

consumer = Consumer(conf)

running = True

def recvideo(consumer, topics):
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                #print(msg.value())
                nparr = np.fromstring(msg.value(), np.float32)
                img_np = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                cv2.imshow("dancer", img_np)
                cv2.waitKey(1)

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def recaudio(consumer, topics):
    fs = 44100

    try:
        consumer.subscribe(topics)
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                print(msg.value())
                nparr = np.fromstring(msg.value(), np.float32)
                sd.play(nparr, fs)


    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def shutdown():
    running = False


recvideo(consumer, ['test-video'])


