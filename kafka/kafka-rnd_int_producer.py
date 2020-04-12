from kafka import KafkaProducer
from random import randrange
import time

# Reference: https://kafka-python.readthedocs.io/en/master/index.html
producer = KafkaProducer(bootstrap_servers=["localhost:9092"])
startTime = time.time()
waitSeconds = 1.0

while True:
    randomInt = randrange(100)
    print("Sending random number to Kafka", randomInt)
    producer.send("test-topic", str(randomInt).encode())

    # Wait a number of second until next message
    time.sleep(waitSeconds - ((time.time() - startTime) % waitSeconds))
