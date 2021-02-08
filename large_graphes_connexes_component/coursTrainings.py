import random

from pyspark import *
# Spark Configurations
conf = SparkConf()
conf.set("spark.master", "local[*]")
conf = conf.setAppName('WordCount')
sc = SparkContext(conf=conf)


# Exercice : # calculer l'intersection entre r1 et r2 sans utiliser 'intersection'
# r1 = sc.parallelize([2, 1, 2, 7, 9, 0, 12, 12])
# r2 = sc.parallelize([5, 1, 2, 8, 1, 9, 0, 13])
#
# r3 = sc.parallelize([(2, 1),(2, 7), (9, 0), (12, 12)])
# listee = r3.reduceByKey(lambda x,y : max(x,y)).collect()
# print(listee)

# r1.intersection(r2).collect()
# print(r1.collect())
# # ---> calculer distinct sans utiliser distinct
#
#
# r1.distinct().collect()
# r1.map(lambda t: (t, 1)).reduceByKey(lambda x, y: x + y).map(lambda x: x[0]).collect()
#
# # calculer l'intersection entre r1 et r2 sans utiliser 'intersection'
#

#
# ####????????????
# # ru = r1.union(r2)
# # ru.collect()
#
#
# # r1.map(lambda t: (t, 1)).reduceByKey(lambda x,y : x+y).join(r2.map(lambda t: (t, 1)).reduceByKey(lambda x,y : x+y)).map(lambda x:x[0]).collect()
#
#
# r11 = r1.map(lambda t: (t, (1, 0)))
# r22 = r2.map(lambda t: (t, (0, 1)))
# ru = r11.union(r22)
# ru.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])).flatMap(
#     lambda x: [x[0]] if x[1][0] > 0 and x[1][1] > 0 else []).collect()
#
# # eliminer le filter, cad remplacer le filter par une autre operation de facon à avoir le meme resultat
#
#
#
# # Par exemple, peut on utiliser le flatMap pour retenir dans r1 que les chaines incluant "bb" ?
#
# # flatMap(lambda x : [x] if x=="*bb*" else [])a=sc.parallelize([('a','1'),('b','2'),('d','3'),('a','2')])
# b= sc.parallelize([('d',2),('e',2)])
# r1 = a.map(lambda x : (x[O],([x[1]],0,1)))
# r2 = b.map(lambda x : (x[O],([x[1]],0,1)))
# result = r1.union(r2).reduceByKey(lambda x , y : (x[0]+y[0],x[1]+y[1],x[2]+y[2])).flatMap(lambda x : ([(x[0],x[1][0][i]) for i in len(x[1][0])] if x[1][1]==0 or x[1][2]==0 else [])).collect()
# print(r1.collect())

# a=sc.parallelize([('a',1),('b',2),('d',3),('a',2)])
# b= sc.parallelize([('d',2),('e',2)])
# r1 = a.map(lambda x : (x[0],([x[1]],0,1)))
# r2 = b.map(lambda x : (x[0],([x[1]],1,0)))
# result = r1.union(r2).reduceByKey(lambda x , y : (x[0]+y[0],x[1]+y[1],x[2]+y[2])).flatMap(lambda x : ([(x[0],x[1][0][i]) for i in range(len(x[1][0]))] if x[1][1]==0 or x[1][2]==0 else [])).collect()
# print(result)


t = sc.parallelize([(1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 12), (8, 9),(9, 20), (10,25)])
t1 = t.map(lambda x : (x[0],[x[1]]))
t2 =t.map(lambda x : (x[0]+1,[x[1]]))
interSect = t1.union(t2).reduceByKey(lambda x,y: x+y).flatMap(lambda x : [x[0]] if len(x[1])>1 and x[1][0]>x[1][1] else [])
print(interSect.collect())


rdd = sc.parallelize([(1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 12), (8, 9),(9, 20), (10,25)])
mape=rdd.map(lambda x:(x[0]-1,x[1]))
mapjoin=rdd.join(mape)
res = mapjoin.flatMap(lambda x:[(x[0]+1,x[1][1])]if x[1][0]<x[1][1]else[]).collect()

print(res)