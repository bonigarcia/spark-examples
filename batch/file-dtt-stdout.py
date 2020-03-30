from pyspark import SparkContext

# Local SparkContext
sc = SparkContext(master="local[*]", appName="file-DTT-stdout")

# 1. Input data: text file
lines = sc.textFile("../data/hamlet.txt")

# 2. Data processing: word count
words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda s: (s, 1))
counts = pairs.reduceByKey(lambda a, b: a + b)
wordcount = counts.collect()

# 3. Output data: show result in the console
print(wordcount)
