from pyspark import SparkContext

# Local SparkContext using N threads (N = number of logical processors)
sc = SparkContext(master="local[*]", appName="range-DTT-stdout")

# 1. Input data: list of integers (unstructured batch)
data = range(1, 10001)
print("Input data: " + str(len(data)) + " integers from " +
      str(data[0]) + " to " + str(data[9999]))

# 2. Data processing
# Parallelize input data into a RDD (lazy evaluation) using * partitions
rangeRDD = sc.parallelize(data)
print("RDD has been created using " +
      str(rangeRDD.getNumPartitions()) + " partitions")

# Process data using lambda functions and collect results:
# 1) substract 1 to all elements. 2) select those lower than 10.
out = (rangeRDD
       .map(lambda y: y - 1)
       .filter(lambda x: x < 10)
       .collect())

# 3. Output data: show result in the standard output (console)
print("The output is " + str(out))
