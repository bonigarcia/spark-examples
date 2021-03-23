from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils

# Local SparkContext and StreamingContext (batch interval of 1 second)
sc = SparkContext(master="local[*]",
                  appName="Flume-DStream-StdOut",
                  conf=SparkConf()
                  .set("spark.jars.packages", "org.apache.spark:spark-streaming-flume_2.11:2.4.7"))
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 1)

# 1. Input data: create a DStream from Apache Flume
stream = FlumeUtils.createStream(ssc, "localhost", 4444)

# 2. Data processing: get first element
lines = stream.map(lambda x: x[1])

# 3. Output data: show result in the console
lines.pprint()

ssc.start()
ssc.awaitTermination()
