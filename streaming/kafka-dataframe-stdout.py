from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("KafkaStructured") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:2181") \
    .option("subscribe", "test-topic") \
    .load()

df.printSchema()

df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
