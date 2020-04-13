from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, udf, StringType
from uuid import uuid4


def writeToCassandra(dataframe, epochId):
    print(f"Writing DataFrame to Cassandra (micro-batch {epochId})")
    randomId = udf(lambda: str(uuid4()), StringType())
    (dataframe.withColumn("id", randomId())
        .write
        .format("org.apache.spark.sql.cassandra")
        .options(keyspace="test", table="wordcount")
        .mode("append")
        .save())


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

# Create DataFrame from socket stream
lines = (spark
         .readStream
         .format("socket")
         .option("host", "localhost")
         .option("port", 9999)
         .load())

# Split the lines into words
words = lines.select(
    explode(
        split(lines.value, " ")
    ).alias("word")
)

# Generate running word count
wordCounts = words.groupBy("word").count()
query = (wordCounts
         .writeStream
         .outputMode("update")
         .trigger(processingTime="10 seconds")
         .foreachBatch(writeToCassandra)
         .start())

query.awaitTermination()
