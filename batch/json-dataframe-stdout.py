from pyspark.sql import SparkSession

# SparkSession (for structured data) in local
spark = (SparkSession
         .builder
         .master("local[*]")
         .appName("JSONfile-DataStream-StdOut")
         .getOrCreate())

# 1. Input data: JSON file
df = spark.read.json("../data/people.json")

df.show()  # Displays the content to stdout
df.printSchema()  # Displays the schema to stdout

# 2. Data processing: different queries
names = df.select("name")
allInc = df.select(df["name"], df["age"] + 1)
older21 = df.where(df["age"] > 21)
countByAge = df.groupBy("age").count()

# 3. Output data: show result in the console
names.show()
allInc.show()
older21.show()
countByAge.show()
