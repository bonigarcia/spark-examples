from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Local SparkContext using * threads, depending on the number of logical processors
sc = SparkContext("local[*]", "NetworkWordCount")

# Create a local StreamingContext with a batch interval of 5 seconds
ssc = StreamingContext(sc, 5)

# Create a DStream that will connect to hostname:port
lines = ssc.socketTextStream("localhost", 9999)

# Split each line into words and count words in each batch
words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Print the word count of each RDD generated in this DStream to the standard output
wordCounts.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
