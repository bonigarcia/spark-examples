from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json


def load_json(msg):
    try:
        return json.loads(msg)
    except Exception:
        print("Exception parsing JSON", msg)
        return {}


def sum_values(list):
    sum = 0
    for i in list:
        sum += i.get("randomInt")
    return sum


# Local SparkContext and StreamingContext
sc = SparkContext(master="local[*]",
                  appName="Kafka-DStream_JSON-StdOut",
                  conf=SparkConf()
                  .set("spark.jars.packages", "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.7"))
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 1)

# 1. Input data: create a DStream from Apache Kafka
stream = KafkaUtils.createStream(
    ssc, "localhost:2181", "spark-streaming-consumer", {"test-topic": 1})

# 2. Data processing: sum receiver integer values
out = (stream
       .map(lambda x: load_json(x[1]))  # parse JSON of Kafka stream value
       .filter(lambda x: len(x) > 0)  # filter out non-json messages
       .map(lambda j: sum_values(j))  # sum each randomInt received
       )

# 3. Output data: show result in the console
out.pprint()

ssc.start()
ssc.awaitTermination()
