from pyspark import SparkContext

# Local SparkContext
sc = SparkContext(master="local[*]", appName="textFile-DTT-StdOut")

# 1. Input data: text file
linesRdd = sc.textFile("../data/hamlet.txt")

# 2. Data processing: word count
wordsRdd = linesRdd.flatMap(lambda line: line.split(" "))
pairsRdd = wordsRdd.map(lambda s: (s, 1))
countsRdd = pairsRdd.reduceByKey(lambda a, b: a + b)
wordcount = countsRdd.collect()

# 3. Output data: show result in the console
print(wordcount)
