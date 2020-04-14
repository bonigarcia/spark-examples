from pyspark import SparkContext, SparkConf
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming import StreamingContext
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS


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

# InfluxDB client (update this info to run this example)
influxClient = InfluxDBClient.from_config_file("../config/influxdb.ini")
bucket = "boni.garcia's Bucket"
org = "boni.garcia@uc3m.es"

# 1. Input data: create a DStream from Apache Kafka
stream = KafkaUtils.createStream(
    ssc, "localhost:2181", "spark-streaming-consumer", {"test-topic": 1})

# 2. Data processing: get numbers
numbers = stream.map(lambda x: x[1])

# 3. Output data: store results in InfluxDb
numbers.foreachRDD(saveToInfluxDB)

ssc.start()
ssc.awaitTermination()
