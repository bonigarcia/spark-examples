from pyspark.sql import SparkSession

# Local SparkSession
spark = (SparkSession
         .builder
         .master("local[*]")
         .appName("Rate-DataFrame-StdOut")
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
         .outputMode("append")
         .format("console")
         .start())

query.awaitTermination()
