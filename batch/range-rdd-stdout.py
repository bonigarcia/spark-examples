from pyspark import SparkContext

# Local SparkContext using N threads (N = number of logical processors)
sc = SparkContext(master="local[*]", appName="range-RDD-stdout")

# 1. Input data: list of integers (unstructured batch)
data = range(1, 10001)
print(f"Input data: {len(data)} integers from {data[0]} to {data[9999]}")

# 2. Data processing
# Parallelize input data into a RDD (lazy evaluation) using * partitions
rangeRDD = sc.parallelize(data)
print(f"RDD has been created using {rangeRDD.getNumPartitions()} partitions")

# Process data using lambda functions and collect results:
# 1) substract 1 to all elements. 2) select those lower than 10.
out = (rangeRDD
       .map(lambda y: y - 1)
       .filter(lambda x: x < 10)
       .collect())

# 3. Output data: show result in the standard output (console)
print(f"The output is {out}")
