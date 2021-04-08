from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

# Local SparkSession
spark = (SparkSession
         .builder
         .master("local[*]")
         .appName("Socket-DataFrame_WordCount-StdOut")
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())

# 1. Input data: DataFrame representing the stream of input lines from socket
df = (spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load())

# 2. Data processing: word count
words = df.select(
    explode(
        split(df.value, " ")
    ).alias("word")
)
wordCount = words.groupBy("word").count()

# 3. Output data: show result in the console
# Print the word count in "complete" mode (entire table)
query = (wordCount
         .writeStream
         .outputMode("complete")
         .format("console")
         .start())

query.awaitTermination()
