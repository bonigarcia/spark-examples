import sys

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print(f"Usage: {sys.argv[0]} <app-name> <stream-name> <endpoint-url> <region-name>",
              file=sys.stderr)
        sys.exit(-1)

    # Local SparkContext and StreamingContext (batch interval of 1 second)
    sc = SparkContext(master="local[*]",
                      appName="Kinesis-DStream-StdOut",
                      conf=SparkConf()
                      .set("spark.jars.packages", "org.apache.spark:spark-streaming-kinesis-asl_2.11:2.4.5"))
    ssc = StreamingContext(sc, 1)

    # 1. Input data: create a DStream from Kinesis
    appName, streamName, endpointUrl, regionName = sys.argv[1:]
    stream = KinesisUtils.createStream(
        ssc, appName, streamName, endpointUrl, regionName, InitialPositionInStream.LATEST, 2)

    # 2. Data processing: word count
    counts = (stream.flatMap(lambda line: line.split(" "))
              .map(lambda word: (word, 1))
              .reduceByKey(lambda a, b: a+b))

    # 3. Output data: show result in the console
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
