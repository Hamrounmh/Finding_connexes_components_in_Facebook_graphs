import time
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
graphe=sc.textFile("/home/bboydhaouse/Bureau/Bigdata/pythonProject/large_graphes_connexes_component/testFiles/twitter_combined.txt")

sortie = graphe.map(lambda x : x.split(' '))
stop =1
acc = sc.accumulator(0)
tps_EUR1 = time.time()

while stop != 0:
    acc.value = 0
    graphe_ccf = sortie.flatMap(lambda x: [(x[0], x[1]), (x[1], x[0])]).groupByKey().map(lambda x : (x[0], list(x[1])))
    sortie = graphe_ccf.flatMap(lambda x: traitement(x[0], x[1]))
    resultat = sortie.collect()
    stop = acc.value


tps_EUR2 = time.time()
executionTime = ((tps_EUR2-tps_EUR1)*1000)
# sortie.saveAsTextFile("/ures/hadoop/sortie.txt")
print(executionTime)
print(resultat)
