from kafka import KafkaProducer
from random import randrange
from math import sin
import time

# Reference: https://kafka-python.readthedocs.io/en/master/index.html
producer = KafkaProducer(bootstrap_servers=["localhost:9092"])
startTime = time.time()
waitSeconds = 1.0
increment = 0.1
counter = 0
amplitude = 50
while True:
    value = amplitude * sin(counter)
    print("Sending number to Kafka", value)
    producer.send("test-topic", str(value).encode())

    # Wait a number of second until next message
    time.sleep(waitSeconds - ((time.time() - startTime) % waitSeconds))
    counter += increment
