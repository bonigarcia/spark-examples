from kafka import KafkaProducer
from random import randrange
import time
import json

producer = KafkaProducer(bootstrap_servers=["localhost:9092"])
startTime = time.time()
waitSeconds = 1.0

while True:
    randomInt1 = randrange(100)
    randomInt2 = randrange(100)
    msg = [{"randomInt": randomInt1}, {"randomInt": randomInt2}]
    print("Sending JSON to Kafka", msg)
    producer.send("test-topic", json.dumps(msg).encode())

    # Wait a number of second until next message
    time.sleep(waitSeconds - ((time.time() - startTime) % waitSeconds))
