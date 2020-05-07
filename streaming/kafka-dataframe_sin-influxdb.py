from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import timezone


def saveDataFreameToInfluxDB(dataframe, batchId):
    for row in dataframe.rdd.collect():
        sinValue = float(row["value"])
        timeValue = row["timestamp"].astimezone(timezone.utc)
        print(f"Writing {sinValue} {timeValue} to InfluxDB")
        point = Point("sine-wave").field("value",
                                         sinValue).time(time=timeValue)
        influxClient.write_api(write_options=SYNCHRONOUS).write(
            bucket=bucket, org=org, record=point)


# Local SparkSession
spark = (SparkSession
         .builder
         .master("local[*]")
         .appName("Kafka-DataFrame-InfluxDB")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# InfluxDB client (update this info to run this example)
influxClient = InfluxDBClient.from_config_file("../config/influxdb.ini")
bucket = "boni.garcia's Bucket"
org = "boni.garcia@uc3m.es"

# 1. Input data: DataFrame from Apache Kafka
df = (spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test-topic")
      .load())
df.printSchema()

# 2. Data processing: read value and timestamp
values = df.selectExpr("CAST(value AS STRING)", "timestamp")

# 3. Output data: store results in InfluxDb
query = (values
         .writeStream
         .outputMode("append")
         .foreachBatch(saveDataFreameToInfluxDB)
         .start())

query.awaitTermination()
