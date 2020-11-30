from pyspark import SparkContext
import json
import datetime
import sys

sc1 = SparkContext.getOrCreate()
review_json = sc1.textFile(sys.argv[1]).map(lambda k: json.loads(k)) 
n = int(sys.argv[5])
partion_no = int(sys.argv[4])
review_json  =review_json.map(lambda x:[x['business_id'],1] )
if(sys.argv[3].strip()== "customized") :
    review_json = review_json.partitionBy(int(partion_no), lambda key : hash(key)).persist()
part = review_json.getNumPartitions()
items =  review_json.glom().map(lambda x : len(x)).collect() 

ans = {
    "n_partitions" : part,
    "n_items" :  items,
    "result": review_json.reduceByKey(lambda x,y : x+y).filter(lambda x : x[1]> n).collect()
}
f = open(sys.argv[2],"w")
json.dump(ans, f)
f.close()
sc1.stop()