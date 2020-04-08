from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, StringType
from uuid import uuid4

spark = (SparkSession
         .builder
         .master("local[*]")
         .appName("JSON-DataFrame-Cassandra")
         .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.11:2.4.3")
         .config("spark.cassandra.connection.host", "localhost")
         .config("spark.cassandra.connection.port", "9042")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# Create DataFrame object from JSON file
people = spark.read.json("../data/people.json", multiLine=True)
people.printSchema()
people.show()

# Write DataFrame in Cassandra including id column
randomId = udf(lambda: str(uuid4()), StringType())
(people.withColumn("id", randomId())
 .write
 .format("org.apache.spark.sql.cassandra")
 .options(keyspace="test", table="people")
 .mode("append")
 .save())

# Read DataFrame from Cassandra
readDf = (spark.read
          .format("org.apache.spark.sql.cassandra")
          .options(keyspace="test", table="people")
          .load())
readDf.printSchema()
readDf.show(truncate=False)
