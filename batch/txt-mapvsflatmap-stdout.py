from pyspark import SparkContext

# Local SparkContext
sc = SparkContext(master="local[*]", appName="textFile-MapVsFlatMap-StdOut")

# 1. Input data: text file
linesRdd = sc.textFile("../data/hamlet.txt")

# 2. Data processing: word count
wordsRddMap = linesRdd.map(lambda line: line.split(" "))
wordsRddFlatMap = linesRdd.flatMap(lambda line: line.split(" "))

# 3. Output data: compare input and output
print("Input: " + str(linesRdd.collect()))
print("Output using map(): " + str(wordsRddMap.collect()))
print("Output using flatMap(): " + str(wordsRddFlatMap.collect()))
