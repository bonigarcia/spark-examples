from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# Local SparkContext and StreamingContext (batch interval of 5 seconds)
sc = SparkContext(master="local[*]",
                  appName="Kafka-DStream-StdOut",
                  conf=SparkConf()
                  .set("spark.jars.packages", "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.5"))
ssc = StreamingContext(sc, 5)

# 1. Input data: create a DStream from Apache Kafka
topic = {"test-topic": 1}  # Topic dictionary (topic-name : num-partitions)
stream = KafkaUtils.createStream(
    ssc, "localhost:2181", "spark-streaming-consumer", topic)

# 2. Data processing: word count
counts = (stream.map(lambda x: x[1])
          .flatMap(lambda line: line.split(" "))
          .map(lambda word: (word, 1))
          .reduceByKey(lambda a, b: a+b))

# 3. Output data: show result in the console
counts.pprint()

ssc.start()
ssc.awaitTermination()
