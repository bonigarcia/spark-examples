from pyspark import SparkContext

# Local SparkContext
sc = SparkContext(master="local[*]", appName="file-DTT-stdout")

# 1. Input data: text file
linesRdd = sc.textFile("../data/hamlet.txt")

# 2. Data processing: word count
out = (linesRdd
       .flatMap(lambda line: line.split(" "))
       .map(lambda word: (word, 1))
       .reduceByKey(lambda x, y: x + y)
       .collect())

# 3. Output data: show result in the console
print(out)
