import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <input-folder> <output-folder>",
              file=sys.stderr)
        sys.exit(-1)

    # Local SparkContext and StreamingContext (batch interval of 5 seconds)
    sc = SparkContext(master="local[*]",
                      appName="FileSystem-DStream-FileSystem")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 5)

    # 1. Input data: create a DStream that read text files from the file system
    inputfolder, outputfolder = sys.argv[1:]
    stream = ssc.textFileStream("file://" + inputfolder)

    # 2. Data processing: word count
    wordCount = (stream.flatMap(lambda line: line.split(" "))
                 .map(lambda word: (word, 1))
                 .reduceByKey(lambda x, y: x + y))

    # 3. Output data: store result as text files
    wordCount.saveAsTextFiles("file://" + outputfolder + "wordcount")

    ssc.start()             # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate
