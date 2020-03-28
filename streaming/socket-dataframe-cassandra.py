from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split


def writeToCassandra(dataframe, epochId):
    dataframe.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(keyspace="test", table="wordcount") \
        .mode("append") \
        .save()


spark = SparkSession \
    .builder \
    .appName("SocketDataFrameCassandra") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.11:2.4.3") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Create DataFrame from socket stream
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Split the lines into words
words = lines.select(
    explode(
        split(lines.value, " ")
    ).alias("word")
)

# Generate running word count
wordCounts = words.groupBy("word").count()
query = wordCounts \
    .writeStream \
    .outputMode("update") \
    .foreachBatch(writeToCassandra) \
    .start()

query.awaitTermination()
