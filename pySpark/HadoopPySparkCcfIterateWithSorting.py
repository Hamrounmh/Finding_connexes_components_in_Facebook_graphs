from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("pyspark")
sc = SparkContext(conf=conf)

iterations = 0
# charger les données
graphe = sc.textFile("twitter_combined.txt")


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

solution = sc.parallelize([result])
solution.saveAsTextFile("sortieCcfIterateSorting")