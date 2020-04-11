from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# Local SparkContext and StreamingContext
sc = SparkContext(master="local[*]",
                  appName="Kafka-DStream-StdOut",
                  conf=SparkConf()
                  .set("spark.jars.packages", "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.5"))
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 1)

# 1. Input data: create a DStream from Apache Kafka
stream = KafkaUtils.createStream(
    ssc, "localhost:2181", "spark-streaming-consumer", {"test-topic": 1})

# 2. Data processing: filter numbers > 50
higher50 = (stream.map(lambda x: x[1])
            .filter(lambda x: int(x) > 50))

# 3. Output data: show result in the console
higher50.pprint()

ssc.start()
ssc.awaitTermination()
