from pyspark.sql import SparkSession
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS


def triangle(x, phase, length, amplitude):
    alpha = (amplitude)/(length/2)
    return -amplitude/2+amplitude*((x-phase) % length == length/2) \
        + alpha*((x-phase) % (length/2))*((x-phase) % length <= length/2) \
        + (amplitude-alpha*((x-phase) % (length/2))) * \
        ((x-phase) % length > length/2)


def saveRowToInfluxDB(rdd):
    value = triangle(rdd["value"], 0, 30, 10)
    print(f"Writing {value} to InfluxDB")
    point = Point("triangle").field("value", value)
    influxClient.write_api(write_options=SYNCHRONOUS).write(
        bucket=bucket, org=org, record=point)


def saveDataFreameToInfluxDB(dataframe, epochId):
    for row in dataframe.rdd.collect():  # To save data in order
        saveRowToInfluxDB(row)


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
org = "boni.garcia@uc3m.es"


# 1. Input data: test DataFrame with sequence and timestamp
df = (spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .load())

# 2. Data processing: nothing

# 3. Output data: show results in the console
query = (df
         .writeStream
         .outputMode("update")
         .format("console")
         .foreachBatch(saveDataFreameToInfluxDB)
         .start())

query.awaitTermination()
