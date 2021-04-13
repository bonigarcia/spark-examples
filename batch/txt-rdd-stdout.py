from pyspark import SparkContext

# Local SparkContext
sc = SparkContext(master="local[*]", appName="textFile-RDD-StdOut")

# 1. Input data: text file
linesRdd = sc.textFile("../data/hamlet.txt")

# 2. Data processing: word count
wordsRdd = linesRdd.flatMap(lambda line: line.split(" "))
pairsRdd = wordsRdd.map(lambda s: (s, 1))
countRdd = pairsRdd.reduceByKey(lambda x, y: x + y)
wordCount = wordCount.collect()

# 3. Output data: show result in the console
print(wordCount)
