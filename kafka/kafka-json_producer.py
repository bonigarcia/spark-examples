from kafka import KafkaProducer
from random import randrange
import time
import json

# Reference: https://kafka-python.readthedocs.io/en/master/index.html
producer = KafkaProducer(bootstrap_servers=[
                         "localhost:9092"], value_serializer=lambda m: json.dumps(m).encode())
startTime = time.time()
waitSeconds = 1.0

while True:
    randomInt1 = randrange(100)
    randomInt2 = randrange(100)
    msg = [{"randomInt": randomInt1}, {"randomInt": randomInt2}]
    print("Sending JSON to Kafka", msg)
    producer.send("test-topic", msg)

    # Wait a number of second until next message
    time.sleep(waitSeconds - ((time.time() - startTime) % waitSeconds))
