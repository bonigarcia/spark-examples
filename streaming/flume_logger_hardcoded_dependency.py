import sys

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(
            "Usage: " + sys.argv[0] + " <hostname> <port>",
            file=sys.stderr)
        sys.exit(-1)

    conf = (SparkConf()
            .setAppName("FlumeLogger")
            .set("spark.jars.packages", "org.apache.spark:spark-streaming-flume_2.11:2.4.5"))
    sc = SparkContext(conf=conf)
    quiet_logs(sc)
    ssc = StreamingContext(sc, 1)

    hostname, port = sys.argv[1:]
    kvs = FlumeUtils.createStream(ssc, hostname, int(port))
    kvs.pprint()

    ssc.start()
    ssc.awaitTermination()
