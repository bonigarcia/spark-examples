from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Local SparkContext
sc = SparkContext(master="local[*]", appName="Socket-DStream_WordCount-StdOut")
sc.setLogLevel("ERROR")

# StreamingContext with a batch interval of 5 seconds
ssc = StreamingContext(sc, 5)

# 1. Input data: create a DStream that receives data from a socket
stream = ssc.socketTextStream("localhost", 9999)

# 2. Data processing: word count
words = stream.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))
wordCount = pairs.reduceByKey(lambda x, y: x + y)

# 3. Output data: show result in the console
# Print the word count of each RDD generated in this DStream
wordCount.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
