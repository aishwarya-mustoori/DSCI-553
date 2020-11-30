from pyspark import SparkContext
import sys
import itertools
import os
from pyspark.sql import SQLContext
from graphframes import GraphFrame
import time
from pyspark import SparkConf

start = time.time()
os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11")

conf=SparkConf()
#conf.set("spark.executor.memory", "4g")
#conf.set("spark.driver.memory", "4g")
sc = SparkContext('local[3]').getOrCreate(conf)

sqlContext = SQLContext(sc)

sc.setLogLevel("ERROR") 
sample_text = sc.textFile(sys.argv[2]).map(lambda x : x.split(','))
sample_first = sample_text.take(1) 
sample_text = sample_text.filter(lambda x : x !=sample_first[0]).map(lambda x : (x[1],[x[0]])).reduceByKey(lambda x,y: x+y)
#edges = sample_text.map(lambda x : (x[0],[x[1])])).reduceByKey(lambda x,y : x+y).map(lambda x :(1,[x])).reduceByKey(lambda x,y : x+y).flatMap(lambda x : itertools.combinations(x[1],2)).filter(lambda x : len(set(x[0][1]).intersection(set(x[1][1]))) >= 7).map(lambda x : (x[0][0].encode('UTF-8'),x[1][0].encode('UTF-8'))).distinct().map(lambda x : [x[0],x[1]])

t = int(sys.argv[1])
edges = sample_text.flatMap(lambda x : ([x[0],y] for y in list(itertools.permutations(x[1],2)))).filter(lambda x : len(set(x[1]))  != 1).map(lambda x : (x[1],[x[0]])).reduceByKey(lambda x,y : x+y).filter(lambda x : len(set(x[1])) >= t).map(lambda x :(x[0][0],x[0][1])).distinct().map(lambda x :[x[0],x[1]])
edgesDF = sqlContext.createDataFrame(edges,["src","dst"])
vertices = edges.flatMap(lambda x: ((x[0]),(x[1]))).distinct().map(lambda x : [x])

verticesDF = sqlContext.createDataFrame(vertices,["id"])

g = GraphFrame(verticesDF, edgesDF)
comm = g.labelPropagation(maxIter=5).select('label','id')
comm_rdd = comm.rdd.map(lambda x:(x[0],[x[1]])).reduceByKey(lambda x,y: x+y).sortBy(lambda x : (len(x[1]),sorted(x[1]))).collect()

f = open(sys.argv[3],"w")
for community in comm_rdd :  
    print(community)
    f.write(str(sorted(community[1])).strip("[]"))
    f.write("\n")
f.close()
sc.stop()
print("Duration : " + str(time.time()-start))



