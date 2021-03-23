from pyspark.sql import SparkSession

# Local SparkSession
spark = (SparkSession
         .builder
         .master("local[*]")
         .appName("Kafka-DataFrame-StdOut")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7")
         .getOrCreate())

# 1. Input data: DataFrame from Apache Kafka
df = (spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test-topic")
      .load())
df.printSchema()

# 2. Data processing: read value
values = df.selectExpr("CAST(value AS STRING)", "timestamp")

# 3. Output data: show result in the console
query = (values
         .writeStream
         .outputMode("append")
         .format("console")
         .start())

query.awaitTermination()
