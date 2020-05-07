from kafka import KafkaProducer
from random import randrange
from random import randrange
from datetime import datetime
from math import sin
import time
import json

producer = KafkaProducer(bootstrap_servers=["localhost:9092"])
startTime = time.time()
waitSeconds = 1.0
sin_counter = 0
sin_increment = 0.1
sin_amplitude = 50

while True:
    time_value = str(datetime.utcnow())
    sin_value = sin_amplitude * sin(sin_counter)
    msg = {"time": time_value, "sin": sin_value}
    print("Sending JSON to Kafka", msg)
    producer.send("test-topic", json.dumps(msg).encode())

    # Wait a number of second until next message
    time.sleep(waitSeconds - ((time.time() - startTime) % waitSeconds))
    sin_counter += sin_increment
