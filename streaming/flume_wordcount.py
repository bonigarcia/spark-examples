from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)


sc = SparkContext(conf=SparkConf()
                  .setAppName("FlumeLogger")
                  .set("spark.jars.packages", "org.apache.spark:spark-streaming-flume_2.11:2.4.5"))
quiet_logs(sc)
ssc = StreamingContext(sc, 1)

kvs = FlumeUtils.createStream(ssc, "localhost", 4444)
lines = kvs.map(lambda x: x[1])
lines.pprint()

ssc.start()
ssc.awaitTermination()
