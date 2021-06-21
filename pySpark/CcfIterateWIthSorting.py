import random
import time

import numpy as np

from pip._vendor.contextlib2 import nullcontext
from pyspark import *
# Spark Configurations
conf = SparkConf()
conf.set("spark.master", "local[*]")
conf = conf.setAppName('WordCount')
sc = SparkContext(conf=conf)







def traitementCcfWIthSecondarySorting(key, values):
    minVal = values[0]
    resultat= []
    if minVal < key:
        resultat.append((key,minVal))
        for value in values:
            if(minVal != value):
                resultat.append((value, minVal))
                acc.add(1)

    return resultat
tps_EUR1 = time.time()
#graphe=sc.textFile("/home/bboydhaouse/Bureau/Bigdata/pythonProject/large_graphes_connexes_component/testFiles/test.txt")
#graphe=sc.textFile("/home/bboydhaouse/Bureau/Bigdata/pythonProject/large_graphes_connexes_component/testFiles/facebook_combined.txt")
graphe=sc.textFile("/home/bboydhaouse/Bureau/Bigdata/pythonProject/large_graphes_connexes_component/testFiles/twitter_combined.txt")


entre=graphe.map(lambda x : x.split(' '))
acc = sc.accumulator(1)
stop=1
while stop !=0:
    acc.value = 0
    grapheCcf = entre.flatMap(lambda x : [(x[0],x[1]),(x[1],x[0])]).groupByKey().map(lambda x: (x[0], sorted(x[1])))
    iterate = grapheCcf.flatMap(lambda x: traitementCcfWIthSecondarySorting(x[0], x[1]))
    dedup=iterate.map(lambda x:((x[0],x[1]),0)).reduceByKey(lambda x,y:x+y).flatMap(lambda x: [(x[0][0],x[0][1])] if x[0][1]!=x[0][0] else [])
    entre=dedup
    result = dedup.collect()
    stop=acc.value

tps_EUR2 = time.time()
executionTime = ((tps_EUR2-tps_EUR1)*1000)

print(executionTime)
print(result)


## CCF itirates with secondary sorting