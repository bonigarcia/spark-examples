from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

# Local SparkSession
spark = (SparkSession
         .builder
         .master("local[*]")
         .appName("Socket-DataFrame-StdOut")
         .config("spark.sql.shuffle.partitions", "20")
         .getOrCreate())

# 1. Input data:  DataFrame representing the stream of input lines from socket
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

# 3. Output data: show result in the console
# Print the word count in "complete" mode (entire table)
query = (wordCounts
         .writeStream
         .outputMode("complete")
         .format("console")
         .start())

query.awaitTermination()
