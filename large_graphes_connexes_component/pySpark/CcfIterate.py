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



def traitement(key,values):
    minval = min(values)
    result=[]
    if minval<key:
        result.append((key,minval))
        for value in values:
            if(minval != value):
                acc.add(1)
                result.append((value,minval))

    return result
# #Projets :

iterations=0
#charger les données
graphe=sc.textFile("/home/bboydhaouse/Bureau/Bigdata/pythonProject/large_graphes_connexes_component/testFiles/facebook_combined.txt")

sortie = graphe.map(lambda x : x.split(' ')).map(lambda x : (x[0],x[1]))
stop =1
acc = sc.accumulator(0)
while stop != 0:
    acc.value = 0
    graphe_direction1 = sortie
    graphe_direction2 = graphe_direction1.map(lambda x: (x[1], x[0]))
    graphe_ccf = graphe_direction1.union(graphe_direction2).groupByKey().mapValues(list)
    sortie = graphe_ccf.flatMap(lambda x: traitement(x[0], x[1]))
    resultat = sortie.collect()
    iterations+=1
    stop = acc.value

# sortie.saveAsTextFile("/ures/hadoop/sortie.txt")
print(stop)
print(resultat)
