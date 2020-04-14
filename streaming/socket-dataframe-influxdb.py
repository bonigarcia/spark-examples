from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import configparser


def saveRddToInfluxDB(rdd):
    count = rdd["count"]
    print(f"Writing {count} to InfluxDB")
    point = Point("wordcount").field("count", count)
    influxClient.write_api(write_options=SYNCHRONOUS).write(
        bucket=bucket, org=org, record=point)


def saveDataFreameToInfluxDB(dataframe, epochId):
    dataframe.rdd.foreach(saveRddToInfluxDB)


# Local SparkSession
spark = (SparkSession
         .builder
         .master("local[*]")
         .appName("Socket-DataFrame-InfluxDB")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# InfluxDB client
config = configparser.ConfigParser()
config.read("../config/influxdb.ini")  # Read config from external file
token = config["influxdb"]["token"]
bucket = config["influxdb"]["bucket"]
org = config["influxdb"]["org"]
influxUrl = config["influxdb"]["influxUrl"]
influxClient = InfluxDBClient(url=influxUrl, token=token, org=org)

# 1. Input data: streaming DataFrame from socket
lines = (spark
         .readStream
         .format("socket")
         .option("host", "localhost")
         .option("port", 9999)
         .load())

# 2. Data processing: word cound
words = lines.select(
    explode(
        split(lines.value, " ")
    ).alias("word")
)
wordCounts = words.groupBy("word").count()


# 3. Output data: store results in InfluxDb
query = (wordCounts
         .writeStream
         .outputMode("update")
         .foreachBatch(saveDataFreameToInfluxDB)
         .start())

query.awaitTermination()
