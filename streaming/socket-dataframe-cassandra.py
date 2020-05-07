from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, udf
from pyspark.sql.types import StringType
from uuid import uuid4


def writeToCassandra(dataframe, batchId):
    print(f"Writing DataFrame to Cassandra (micro-batch {batchId})")
    randomId = udf(lambda: str(uuid4()), StringType())
    (dataframe.withColumn("id", randomId())
        .write
        .format("org.apache.spark.sql.cassandra")
        .options(keyspace="test", table="wordcount")
        .mode("append")
        .save())


# Local SparkSession
spark = (SparkSession
         .builder
         .master("local[*]")
         .appName("Socket-DataFrame-Cassandra")
         .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.11:2.4.3")
         .config("spark.cassandra.connection.host", "localhost")
         .config("spark.cassandra.connection.port", "9042")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# 1. Input data: Create DataFrame from socket stream
lines = (spark
         .readStream
         .format("socket")
         .option("host", "localhost")
         .option("port", 9999)
         .load())

# 2. Data processing: word count
words = lines.select(
    explode(
        split(lines.value, " ")
    ).alias("word")
)
wordCounts = words.groupBy("word").count()

# 3. Output data: write results to Cassandra
query = (wordCounts
         .writeStream
         .outputMode("update")
         .foreachBatch(writeToCassandra)
         .start())

query.awaitTermination()
