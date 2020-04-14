from pyspark import SparkContext, SparkConf
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import configparser


def saveToInfluxDB(rdd):
    data = rdd.collect()
    if len(data) == 1:
        sinValue = float(data[0])
        print(f"Writing {sinValue} to InfluxDB")
        point = Point("sine-wave").field("value", sinValue)
        influxClient.write_api(write_options=SYNCHRONOUS).write(
            bucket=bucket, org=org, record=point)


# Local SparkContext and StreamingContext
sc = SparkContext(master="local[*]",
                  appName="Kafka-DStream_SinWave-InfluxDB",
                  conf=SparkConf()
                  .set("spark.jars.packages", "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.5"))
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 1)

# InfluxDB client
config = configparser.ConfigParser()
config.read("../config/influxdb.ini")  # Read config from external file
token = config["influxdb"]["token"]
bucket = config["influxdb"]["bucket"]
org = config["influxdb"]["org"]
influxUrl = config["influxdb"]["influxUrl"]
influxClient = InfluxDBClient(
    url="InfluxDB URL", token="my token", org="my org")

# 1. Input data: create a DStream from Apache Kafka
stream = KafkaUtils.createStream(
    ssc, "localhost:2181", "spark-streaming-consumer", {"test-topic": 1})

# 2. Data processing: get numbers
numbers = stream.map(lambda x: x[1])

# 3. Output data: store results in InfluxDb
numbers.foreachRDD(saveToInfluxDB)

ssc.start()
ssc.awaitTermination()
