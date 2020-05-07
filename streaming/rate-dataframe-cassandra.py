from pyspark.sql import SparkSession


def writeToCassandra(dataframe, batchId):
    print(f"Writing DataFrame to Cassandra (micro-batch {batchId})")
    (dataframe
        .write
        .format("org.apache.spark.sql.cassandra")
        .options(keyspace="test", table="seq")
        .mode("append")
        .save())


# Local SparkSession
spark = (SparkSession
         .builder
         .master("local[*]")
         .appName("Rate-DataFrame-Cassandra")
         .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.11:2.4.3")
         .config("spark.cassandra.connection.host", "localhost")
         .config("spark.cassandra.connection.port", "9042")
         .getOrCreate())

# 1. Input data: test DataFrame with sequence and timestamp
df = (spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .load())

# 2. Data processing: filter odd values
even = df.filter(df["value"] % 2 == 0)

# 3. Output data: show results in the console
query = (even
         .writeStream
         .outputMode("update")
         .format("console")
         .foreachBatch(writeToCassandra)
         .start())

query.awaitTermination()
