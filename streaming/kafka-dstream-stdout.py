from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


sc = SparkContext(master="local[*]",
                  appName="Kafka-DStream-StdOut",
                  conf=SparkConf()
                  .set("spark.jars.packages", "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.5"))
ssc = StreamingContext(sc, 5)

kvs = KafkaUtils.createStream(
    ssc, "localhost:2181", "spark-streaming-consumer", {"test-topic": 1})

lines = kvs.map(lambda x: x[1])
counts = lines.flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a+b)

counts.pprint()

ssc.start()
ssc.awaitTermination()
