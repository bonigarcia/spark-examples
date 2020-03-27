import sys

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(
            "Usage: " + sys.argv[0] + " <zk> <topic>",
            file=sys.stderr)
        sys.exit(-1)

    conf = (SparkConf()
            .setAppName("KafkaWordCount")
            .set("spark.jars.packages", "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.5"))
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 5)

    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(
        ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)

    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
