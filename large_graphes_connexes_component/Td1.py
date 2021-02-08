import random

from pyspark import *
# Spark Configurations
conf = SparkConf()
conf.set("spark.master", "local[*]")
conf = conf.setAppName('WordCount')
sc = SparkContext(conf=conf)



#charger les données
customer=sc.textFile("/home/bboydhaouse/PycharmProjects/pythonProject/Files/Customer.txt")
order=sc.textFile("/home/bboydhaouse/PycharmProjects/pythonProject/Files/Order.txt")

# if __name__ == '__main__':
    # q1 = customer.map(lambda x: x.split(',')).map(lambda x: (x[1],x[2])).filter(lambda x : x[0].split('/')[1] == '07').map(lambda x : x[1]).distinct().collect()
   # print(q1)

    # ordertotal = order.map(lambda x : x.split(',')).map(lambda x : (x[0],int(x[1]))).reduceByKey(lambda x ,y : x + y)
    # ordercount = order.map(lambda x : x.split(',')).map(lambda x : (x[0],x[1])).distinct().map(lambda x : (x[0],1)).reduceByKey(lambda x ,y : x + y).filter(lambda x : x[1]>1)
    # q2=ordertotal.join(ordercount)
    # print(q2.collect())


#Q4
    #
    # orders = order.map(lambda x : x.split(',')).map(lambda x: (x[0],x[1]))
    # customers = customer.map(lambda x : x.split(',')).map(lambda x: (x[0],x[1])).filter(lambda x : x[1][3:5] == '07')
    # fusion = orders.join(customers).map(lambda x: (x[0],x[1][0])).reduceByKey(lambda x,y : int(x)+int(y))
    # print(fusion.collect())

#Exercice 2
# words=sc.textFile("/home/bboydhaouse/Bureau/Bigdata/pythonProject/Files/5000-8.txt")
#
#
# liste = words.flatMap( lambda x : x.strip().split(' ')).filter(lambda x: x!='').map(lambda x: (x,1)).reduceByKey(lambda x,y : x+y ).count()
# print(liste)
# acc = sc.accumulator(0)
#
# def Uniqueword(tableau):
#     result = len(tableau)
#     acc.add(1)
#     return result
#
# liste2_1 = words.map(lambda x : x.split(' ')).map(lambda xflatMap(lambda x : (x[0],1)). : Uniqueword(x)).collect()
# print(liste2_1)

#Exercice 3

# pets = sc.parallelize([("cat", 1), ("dog", 1), ("cat", 2),("dog", 3) ])
# nbpets=pets.keys().distinct().count()
# liste = pets.reduceByKey(lambda x,y : x+y).map(lambda x: (x[0],float(x[1]/nbpets))).collect()
# print(liste)
#

# Exercice 4

#construction des fichier F1, F2
# f1_tmp = sc.parallelize([random.randint(1,100) for x in range(3)])
# f1=f1_tmp.map(lambda x : [random.random()* x for  i in range(5)] )
#
#
#
# f2_tmp = sc.parallelize( [random.randint(1,100) for x in range(2)])
# f2=f2_tmp.map(lambda x : [random.random()* x for  i in range(5)] )


#pcomment faire le produit cartésien roduit cartésien
# def constuire(x):
#     res=[]
#     for i in range(2):
#         res.append((i,x))
#     return res

# f1_toJoin= f1.flatMap(lambda x: constuire(x))
# f2_toJoin = f2.flatMap(lambda x: constuire(x))
# product = f1_toJoin.join(f2_toJoin).map(lambda x : str(x[1][0]) + " ------  "+str(x[1][1])).collect()
# print(product)

#EXERCICE 7
# visits = sc.parallelize([("h", "1.2.3.4"), ("a", "3.4.5.6"), ("h", "1.3.3.1"),("x", "Other")])
# pageNames = sc.parallelize([("h", "Home"), ("a", "About"), ("o", "Other")])
# li=visits.join(pageNames).collect()
# print(li)

# # Exercice 8 :
# graph=sc.textFile("/home/bboydhaouse/Bureau/Bigdata/pythonProject/Files/graph.txt")
# graph=graph.distinct()
# lfrom=graph.map(lambda x : x.split()).map(lambda x: (x[0],1))
# lto=graph.map(lambda x : x.split()).map(lambda x: (x[2],1))
# sinks = lto.subtractByKey(lfrom).distinct().collect()
# # universal_sinks=
# nbsommet = graph.map(lambda x : x[0]).distinct().count()-1
# universal_sinks=lto.subtractByKey(lfrom).reduceByKey(lambda x,y: x+y).filter(lambda x: x[1]>nbsommet).map(lambda x: x[0]).collect()
# print(universal_sinks)


#Projets :
