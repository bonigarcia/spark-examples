import sys

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print(
            "Usage: " + sys.argv[0] + " <app-name> <stream-name> <endpoint-url> <region-name>",
            file=sys.stderr)
        sys.exit(-1)

    conf = (SparkConf()
         .setAppName("KinesisWordCount")
         .set("spark.jars.packages", "org.apache.spark:spark-streaming-kinesis-asl_2.11:2.4.5"))
    sc = SparkContext(conf = conf)
    ssc = StreamingContext(sc, 1)
    appName, streamName, endpointUrl, regionName = sys.argv[1:]
    lines = KinesisUtils.createStream(
        ssc, appName, streamName, endpointUrl, regionName, InitialPositionInStream.LATEST, 2)
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)
    counts.pprint()

    ssc.start()
