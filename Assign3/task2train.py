from pyspark import SparkContext 
import sys
import json
import string
from collections import defaultdict
import math
import time 
import gc
from pyspark import SparkConf
def removePunctuation(stringg,excludedWords) : 
    stringg = stringg.translate(str.maketrans('', '', string.punctuation))
    text = []
    for words in stringg.split(" "): 
        if(words !="" and words !=None and words not in excludedWords and not words.isdigit()):
            text.append(words)
    return text

def tfIDF(business,text):
    
    dictionary = defaultdict(int)
    for word in text : 
        dictionary[word]  = dictionary[word]  + 1
    max_words = max(dictionary.values())
    tfIDF_dict={}
    for word,frequency in dictionary.items(): 
        tf = frequency/max_words
        idf = math.log(no_of_words/for_idf[word],2)
        tfIDFScore = tf*idf
        tfIDF_dict[word]= tfIDFScore
    tfIDF_dict = sorted(tfIDF_dict.items(),key=lambda x :x[1],reverse=True)

    return [business,[(i[0],dictionary[i[0]])for i in tfIDF_dict[:200]]]

def words(businesses): 
    dictionary = defaultdict(int)
    for business in businesses :   
        text = words_for_business1[business]
        for word in text : 
            dictionary[word[0]]  = dictionary[word[0]] + word[1]  
    return [i[0] for i in sorted(dictionary.items(),key=lambda x :x[1],reverse=True)][:200]

start = time.time()
conf=SparkConf()
conf.set("spark.executor.memory", "4g")
conf.set("spark.driver.memory", "4g")
sc = SparkContext().getOrCreate(conf)
file1 = open(sys.argv[3],"r")
excluded = []
x  = file1.readline()
while(x != "") : 
    excluded.append(x.lower().strip())
    x = file1.readline()
file1.close()
input = sc.textFile(sys.argv[1]).map(lambda x : json.loads(x)).cache()
input = input.map(lambda x : (x['business_id'],x['user_id'],removePunctuation(x['text'].lower(),excluded)))
all_words = input.flatMap(lambda x :x[2]).map(lambda x :(x,1)).reduceByKey(lambda x,y : x+y).cache()
no_of_words= all_words.count()
business_buckets = input.map(lambda x :(x[0],x[2])).reduceByKey(lambda x,y : x+y).cache()
for_idf = business_buckets.flatMap(lambda x : [(y,1) for y in set(x[1])]).reduceByKey(lambda x,y:x+y).collectAsMap()
words_for_business1 = business_buckets.map(lambda x : (tfIDF(x[0],x[1])))
words_for_business = words_for_business1.map(lambda x : (x[0],[y[0] for y in x[1]])).collectAsMap()
words_for_business1 = words_for_business1.collectAsMap()
all_words.unpersist()
del all_words
business_buckets.unpersist()
del business_buckets
user_profiles = input.map(lambda x : [x[1],[x[0]]]).reduceByKey(lambda x,y : x+y)
users = user_profiles.map(lambda x:(x[0],words(x[1]))).collectAsMap()
input.unpersist()
del input
ans  ={
    "userProfiles": users,
    "businessProfiles": words_for_business
}
f = open(sys.argv[2],"w")
json.dump(ans,f)
f.close()
sc.stop()
end = time.time()
print("Duration "+str(end-start))