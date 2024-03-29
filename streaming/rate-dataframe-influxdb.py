from pyspark.sql import SparkSession
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from datetime import timezone


def triangle(x, phase, length, amplitude):
    alpha = (amplitude)/(length/2)
    return -amplitude/2+amplitude*((x-phase) % length == length/2) \
        + alpha*((x-phase) % (length/2))*((x-phase) % length <= length/2) \
        + (amplitude-alpha*((x-phase) % (length/2))) * \
        ((x-phase) % length > length/2)


def saveDataFrameToInfluxDB(dataframe, batchId):
    for row in dataframe.rdd.collect():
        tr = row["tr"]
        ts = row["timestamp"].astimezone(timezone.utc)
        print(f"Writing {tr} to InfluxDB (timestamp {ts})")
        point = Point("trwave").field("tr", tr).time(time=ts)
        influxClient.write_api(write_options=SYNCHRONOUS).write(
            bucket=bucket, record=point)


# Local SparkSession
spark = (SparkSession
         .builder
         .master("local[*]")
         .appName("Rate-DataFrame-InfluxDB")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# InfluxDB client (update this info to run this example)
influxClient = InfluxDBClient.from_config_file("../config/influxdb.ini")
bucket = "boni.garcia's Bucket"


# 1. Input data: test DataFrame with sequence and timestamp
df = (spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .load())

# 2. Data processing: add new column with the value of a triangle wave
trwave = udf(lambda x: triangle(x, 0, 30, 10), FloatType())
triangleDf = df.withColumn("tr", trwave(df["value"]))

# 3. Output data: show results in the console
query = (triangleDf
         .writeStream
         .outputMode("append")
         .foreachBatch(saveDataFrameToInfluxDB)
         .start())

query.awaitTermination()
