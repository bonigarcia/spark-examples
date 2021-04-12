from pyspark import SparkContext, SparkConf
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime
import json


def saveToInfluxDB(rdd):
    data = rdd.collect()
    for i in data:
        sinValue = float(i.get("sin"))
        timeValue = datetime.strptime(i.get("time"), "%Y-%m-%d %H:%M:%S.%f")
        print(f"Writing {sinValue} {timeValue} to InfluxDB")
        point = Point("sine-wave").field("value",
                                         sinValue).time(time=timeValue)
        influxClient.write_api(write_options=SYNCHRONOUS).write(
            bucket=bucket, record=point)


# Local SparkContext and StreamingContext
sc = SparkContext(master="local[*]",
                  appName="Kafka-DStream_SinWave-InfluxDB",
                  conf=SparkConf()
                  .set("spark.jars.packages", "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.7"))
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 1)

# InfluxDB client (update this info to run this example)
influxClient = InfluxDBClient.from_config_file("../config/influxdb.ini")
bucket = "boni.garcia's Bucket"

# 1. Input data: create a DStream from Apache Kafka
stream = KafkaUtils.createStream(
    ssc, "localhost:2181", "spark-streaming-consumer", {"test-topic": 1})

# 2. Data processing: get JSON values
out = (stream.map(lambda x: json.loads(x[1])))

# 3. Output data: store results in InfluxDb
out.foreachRDD(saveToInfluxDB)

ssc.start()
ssc.awaitTermination()
