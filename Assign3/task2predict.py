from pyspark import SparkContext
import json 
import math
import sys
import time
def cosineSimilarity(x): 
    user = x['user_id']
    business=x['business_id']
    if(business_profiles.get(business) == None or user_profiles.get(user) == None ): 
        return {"user_id":user,"business_id":business,"sim":0}
    user_words= user_profiles[user]
    business_words = business_profiles[business]
    
    intersect = set(user_words).intersection(set(business_words))
    cosine = len(intersect)/(math.sqrt(len(business_words))* math.sqrt(len(business_words)))
    return {"user_id":user,"business_id":business,"sim":cosine}
start = time.time()

sc = SparkContext.getOrCreate()
f = open(sys.argv[2],"r")
input_json = json.loads(f.readline())
user_profiles = input_json['userProfiles']
business_profiles = input_json['businessProfiles']
predict = sc.textFile(sys.argv[1]).map(lambda x : json.loads(x)).map(lambda x :cosineSimilarity(x)).filter(lambda x: x["sim"]>=0.01).collect()
del user_profiles
del input_json
del business_profiles
f = open(sys.argv[3],"w")
for value in predict:
    json.dump(value, f)
    f.write("\n")
f.close()
sc.stop()
end = time.time()
print("Duration "+str(end-start))