from pyspark import SparkConf, SparkContext

# Create SparkContext in local using * threads, depending on the number of logical processors
sc = SparkContext("local[*]", "HelloWorld")

# Input data: integer list (unstructured batch)
data = range(1, 10001)
print("Input data has " + str(len(data)) + " integers from " +
      str(data[0]) + " to " + str(data[9999]))

# Parallelize input data using * partitions
# This operation is a transformation of data into an RDD.
# Spark uses lazy evaluation, so no Spark jobs are run at this point
rangeRDD = sc.parallelize(data)
print("RDD has been created using " +
      str(rangeRDD.getNumPartitions()) + " partitions")

# Process data using different lambda functions:
# substract 1 to all elements and then select those lower than 10
out = (rangeRDD
       .map(lambda y: y - 1)
       .filter(lambda x: x < 10)
       .collect())
print("The output is " + str(out))
