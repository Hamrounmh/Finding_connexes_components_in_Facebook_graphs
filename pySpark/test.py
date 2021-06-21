from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("pyspark")
sc = SparkContext(conf=conf)

dataRDD = sc.textFile("test.txt")
for line in dataRDD.collect():
    print(line)
dataRDD.saveAsTextFile("sortieTestFile")