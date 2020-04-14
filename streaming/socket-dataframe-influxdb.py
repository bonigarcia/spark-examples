from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS


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

# InfluxDB client (update this info to run this example)
influxClient = InfluxDBClient.from_config_file("../config/influxdb.ini")
bucket = "boni.garcia's Bucket"
org = "boni.garcia@uc3m.es"

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
