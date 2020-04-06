from pyspark.sql import SparkSession

# Local SparkSession
spark = (SparkSession
         .builder
         .master("local[*]")
         .appName("Kafka-DataFrame-StdOut")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5")
         .getOrCreate())

# 1. Input data: DataFrame from Apache Kafka
df = (spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:2181")
      .option("subscribe", "test-topic")
      .load())

df.printSchema()
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
