from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import random
import re
import time
import statistics
import binascii
import sys
import json

start = time.time()
sc = SparkContext.getOrCreate()
sc.setLogLevel("ERROR")
textFile = sc.textFile(sys.argv[1]).map(lambda x: json.loads(x)).map(lambda x: x["name"]).distinct()

def applyHash(name):
    x = int(binascii.hexlify(name.encode('utf8')), 16)
    ans = set()
    for i in range(len(hashFunctions_a)):
        value = ((hashFunctions_a[i]*x + hashFunctions_b[i])%p)%m
        ans.add(value)
    return list(ans)

m = 500000
p = 2454587
random.seed(399)
hashFunctions_a = random.sample(range(1,p),2)
hashFunctions_b = random.sample(range(1,p),2)



bloom_filter_input = textFile.filter(lambda x: x != None and len(x)>0).flatMap(lambda x : applyHash(x)).map(lambda x: (x,1)).reduceByKey(lambda x,y : x+y).collectAsMap()
print(len(bloom_filter_input))
ans = []
f = open(sys.argv[2],"r")
lines = f.readlines()

for line in lines:
    jsons = json.loads(line)
    name = jsons["name"]
    if (len(name)>0 and name != None):
        hash = applyHash(name)
        flag = True
        for i in hash : 
            if(bloom_filter_input.get(i) ==None) : 
                flag = False 
                break
        if flag : 
            ans.append("T")
        else : 
            ans.append("F")
    else : 
        ans.append("T")
f = open(sys.argv[3],"w")
f.write(str(ans).strip("[]").replace(", "," ").replace("'",""))

f.close()
sc.stop()
print("Duration"+str(time.time()-start))
