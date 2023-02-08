from pyspark import SparkContext

sc = SparkContext(master='local', appName='jan').getOrCreate()

rdd = sc.parallelize(i for i in range(1,11))
rdd.foreach(print)
print('************************************')
rdd1= sc.parallelize(i for i in range(5,16))
rdd1.foreach(print)
print('*****************************')

rdd2 = rdd.union(rdd1)
rdd2.foreach(print)

print('***********************')

rdd3 = rdd2.distinct()
rdd3.foreach(print)