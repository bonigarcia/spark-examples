from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with * working thread and batch interval of 5 seconds
sc = SparkContext("local[*]", "NetworkWordCount")
ssc = StreamingContext(sc, 5)

# Create a DStream that will connect to hostname:port
lines = ssc.socketTextStream("localhost", 9999)

# Split each line into words
words = lines.flatMap(lambda line: line.split(" "))

# Count each word in each batch
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
