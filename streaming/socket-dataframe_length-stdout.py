from pyspark.sql import SparkSession
from pyspark.sql.functions import length

# Local SparkSession
spark = (SparkSession
         .builder
         .master("local[*]")
         .appName("Socket-DataFrame_Length-StdOut")
         .getOrCreate())

# 1. Input data: DataFrame representing the stream of input lines from socket
df = (spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load())

# 2. Data processing: add column with the length of each line
dfLen = df.withColumn("length", length(df["value"]))

# 3. Output data: show result in the console
query = (dfLen
         .writeStream
         .outputMode("append")
         .format("console")
         .start())

query.awaitTermination()
