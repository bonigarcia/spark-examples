from kafka import KafkaProducer
from random import randrange
from math import sin
import time

producer = KafkaProducer(bootstrap_servers=["localhost:9092"])
startTime = time.time()
waitSeconds = 1.0
sin_counter = 0
sin_increment = 0.1
sin_amplitude = 50

while True:
    value = sin_amplitude * sin(sin_counter)
    print("Sending sin value to Kafka", value)
    producer.send("test-topic", str(value).encode())

    # Wait a number of second until next message
    time.sleep(waitSeconds - ((time.time() - startTime) % waitSeconds))
    sin_counter += sin_increment
