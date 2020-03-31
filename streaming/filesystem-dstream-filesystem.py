import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(
            "Usage: " + sys.argv[0] + " <input-folder> <output-folder>",
            file=sys.stderr)
        sys.exit(-1)

    # Local SparkContext and StreamingContext (batch interval of 10 seconds)
    sc = SparkContext(master="local[*]",
                      appName="FileSystem-DStream-FileSystem")
    ssc = StreamingContext(sc, 10)

    # 1. Input data: create a DStream that read text files from the file system
    inputfolder, outputfolder = sys.argv[1:]
    lines = ssc.textFileStream("file://" + inputfolder)

    # 2. Data processing: word count
    words = lines.flatMap(lambda line: line.split(" "))
    pairs = words.map(lambda word: (word, 1))
    wordCounts = pairs.reduceByKey(lambda x, y: x + y)

    # 3. Output data: store result as text files
    wordCounts.saveAsTextFiles("file://" + outputfolder + "wordcount")

    ssc.start()             # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate
