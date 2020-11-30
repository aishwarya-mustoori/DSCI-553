
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import sys
import json
import random
import time 
import binascii
import random
from datetime import datetime

from pyspark import SparkConf


def FMAlgo(x):
    start_time = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    groud_truth = x.distinct().collect()
    zeros = []
    print(groud_truth)
    for value in groud_truth : 
        hexVal = int(binascii.hexlify(value.encode('utf8')),16)
        ans = list()
        for i in range(len(hashFunctions_a)):
            hashVal = ((hashFunctions_a[i]*hexVal + hashFunctions_b[i])%hashFunctions_p[i])%m
            ans.append(bin(hashVal))
        zeros.append(ans)
    y =[]
    #Use the max of all hash functions 
    for i in range(len(hashFunctions_a)) : 
        max_xeros = -float("inf")
        for zero in zeros : 
            x = len(zero[i])-len(str(zero[i]).rstrip("0"))
            max_xeros = max(x,max_xeros)
        # Put 2 power R in main result
        y.append(pow(max_xeros,2))
    
    #Find Mean
    #Find Median 
    print(y)
    
    average = []
    groups = 5 #logn
    for i in range(0,len(hashFunctions_a),groups): 
        sum_avg = sum(y[i:i+groups])/groups
        average.append(sum_avg)
    average.sort()
    median = average[int((len(hashFunctions_a)/(2*groups)))]
    
    f = open(sys.argv[2],"a")
    f.write(str(start_time)+","+str(len(groud_truth))+","+str(median))

    print(str(start_time)+","+str(len(groud_truth))+","+str(median)+","+str(abs((len(groud_truth)-median)/len(groud_truth))))
    f.write("\n")
    f.close()

 
f = open(sys.argv[2],"w")
f.write("Time,Ground Truth,Estimation")
f.write("\n")
f.close()
conf = SparkConf().setAppName("hw5").setMaster("local[3]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
sc.setSystemProperty('spark.driver.memory', '4g')
sc.setSystemProperty('spark.executor.memory', '4g')

m = 500000
p = 2454587
random.seed(399)
hashFunctions_a = [578, 2564, 264, 3815, 1559, 2565, 1477, 8750, 915, 727, 4086, 6271, 5852, 8762, 8054, 793, 8100, 2819, 7831, 5827, 9433, 285, 3317, 1678, 5006, 6778, 5465, 8757, 7182, 1856, 3995, 5129, 4106, 1337, 5449]
hashFunctions_b = [1513, 1429, 8826, 347, 1070, 9638, 7566, 7379, 5802, 9347, 9654, 7002, 5079, 3913, 8834, 7314, 9433, 3072, 2518, 9698, 7453, 5610, 296, 7281, 7629, 7374, 417, 9453, 4590, 9259, 1249, 4845, 4992, 6224, 277]
hashFunctions_p = [2276954, 1047918, 688148, 34220, 2387410, 1600048, 249855, 1470353, 1806470, 1557500, 1854053, 606987, 2045999, 2188392, 1681256, 1364234, 749551, 1883710, 1883725, 2038449, 2197411, 1565199, 2372069, 764976, 449126, 1128008, 1643967, 242369, 2451109, 2003795, 53337, 639986, 1285772, 1977587, 1150655]
# hashFunctions_a = random.sample(range(1,10000),35)
# hashFunctions_b = random.sample(range(1,10000),35)
# hashFunctions_p = random.sample(range(10000,p),35)
# print(hashFunctions_a)
# print(hashFunctions_b)
# print(hashFunctions_p)

ssc=StreamingContext(sc , 5)
# Apply the FM Algorithm on stream data 
fmAlgo = ssc.socketTextStream('localhost', int(sys.argv[1])).map(lambda x:json.loads(x)).map(lambda x: x['state']).window(30,10).foreachRDD(lambda x : FMAlgo(x)) 
               
ssc.start()
ssc.awaitTermination()