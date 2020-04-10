from pyspark import SparkContext

# Local SparkContext
sc = SparkContext(master="local[*]", appName="textFile-MapVsFlatMap-StdOut")

# 1. Input data: text file
linesRdd = sc.textFile("../data/hamlet.txt")

# 2. Data processing: word count
wordsRddMap = linesRdd.map(lambda line: line.split(" "))
wordsRddFlatMap = linesRdd.flatMap(lambda line: line.split(" "))

# 3. Output data: compare input and output
print("Input", linesRdd.collect())
print("Output using map()", wordsRddMap.collect())
print("Output using flatMap()", wordsRddFlatMap.collect())
