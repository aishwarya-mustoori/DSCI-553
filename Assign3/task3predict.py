from pyspark import SparkContext
import sys
import json
from pyspark import SparkConf
import math

conf=SparkConf()
conf.set("spark.executor.memory", "4g")
conf.set("spark.driver.memory", "4g")
sc = SparkContext().getOrCreate(conf)

  
def getPredictedRatings(x,users_dict,business_dict,businessUserRatingMap,businessUserMap) : 
    rating = 0
    user = x[0] 
    business = x[1] 
    if(users_dict.get(user) != None and business_dict.get(business) != None ):
        user = users_dict[user]
        business = business_dict[business]
        all_business = businessUserMap[user]
        weights = 0 
        corated = 0
        user_dict= {}
        for bus in all_business : 
            if(train_model_map.get(tuple(sorted([business,bus]))) != None) : 
                user_dict[bus] = train_model_map[tuple(sorted([bus,business]))]  
                      
        businesses = sorted(user_dict.items(),key=lambda x: x[1],reverse=True)
        for business1 in businesses[:10]: 
            sim = business1[1]
            corated = corated + ((businessUserRatingMap[(business1[0],user)])* sim) 
            weights = weights + sim
        if(weights!=0):
            rating = corated/weights
            return rating
    return 0

def getPredictedRatings1(x,business_dict,users_dict,businessUserRatingMap,businessUserMap,userAverageMap) : 
    rating = 0
    business = x[0] 
    user = x[1] 
    if(users_dict.get(user) != None and business_dict.get(business) != None ):
        
        user = users_dict[user]
        business = business_dict[business]
        all_users = businessUserMap[business]
        user_dict= {}
        for users in all_users : 
            if(train_model_map.get(tuple(sorted([users,user]))) != None) : 
                user_dict[users] = train_model_map[tuple(sorted([users,user]))]  
                         
        userses = sorted(user_dict.items(),key=lambda x: x[1],reverse=True)
        avg = 0
        length = 0 
        weights = 0 
        corated = 0
        for users in userses[:10]: 
            sim = users[1]
            length = length + 1
            corated = corated + ((businessUserRatingMap[(users[0],business)]- userAverageMap[users[0]])* sim) 
            weights = weights + sim
        rating = userAverageMap[user]
        if(weights !=0) : 
            rating = rating + corated/weights
        return rating
    return 0

if(sys.argv[5]== "item_based"):
    #Train File 
    train_input = sc.textFile(sys.argv[1]).map(lambda x : json.loads(x))
    distinct_users = train_input.map(lambda x : x["user_id"]).distinct()
    user_dict = {}
    index = 0
    for values in distinct_users.collect() : 
        user_dict[index]= values
        user_dict[values] = index
        index = index + 1

    distinct_business = train_input.map(lambda x : x["business_id"]).distinct()
    business_dict = {}
    index = 0
    for values in distinct_business.collect() : 
        business_dict[index]= values
        business_dict[values] = index
        index = index + 1
    train_input = train_input.map(lambda x : ((business_dict[x['business_id']],user_dict[x['user_id']]),x['stars']))

    businessUserRatingMap = train_input.collectAsMap()
    businessUserMap = train_input.map(lambda x:(x[0][1],[x[0][0]])).reduceByKey(lambda x,y : x+y).collectAsMap()
    
    #Model_file 

    train_model_map= sc.textFile(sys.argv[3]).map(lambda x :json.loads(x)).map(lambda  x: (tuple(sorted([business_dict[x["b1"]],business_dict[x["b2"]]])),x["sim"])).collectAsMap()
    
    #test_file 
    text_file = sc.textFile(sys.argv[2]).map(lambda x : json.loads(x)).map(lambda x :(x['user_id'],x['business_id'])).map(lambda x : {"user_id":x[0],"business_id":x[1],"stars":getPredictedRatings(x,user_dict,business_dict,businessUserRatingMap,businessUserMap)}).filter(lambda x : x["stars"]>0).collect()
    f = open(sys.argv[4],"w")
    for value in text_file:
        json.dump(value,f)
        f.write("\n")
else : 
    train_input = sc.textFile(sys.argv[1]).map(lambda x : json.loads(x))
    distinct_users = train_input.map(lambda x : x["user_id"]).distinct()
    user_dict = {}
    index = 0
    for values in distinct_users.collect() : 
        user_dict[index]= values
        user_dict[values] = index
        index = index + 1

    distinct_business = train_input.map(lambda x : x["business_id"]).distinct()
    business_dict = {}
    index = 0
    for values in distinct_business.collect() : 
        business_dict[index]= values
        business_dict[values] = index
        index = index + 1
    train_input = train_input.map(lambda x : ((user_dict[x['user_id']],business_dict[x['business_id']]),x['stars']))

    businessUserRatingMap = train_input.collectAsMap()
    businessUserMap = train_input.map(lambda x:(x[0][1],[x[0][0]])).reduceByKey(lambda x,y : x+y).collectAsMap()
    userAverageMap = train_input.map(lambda x:(x[0][0],(x[1],1))).reduceByKey(lambda x,y : (x[0]+y[0],y[1]+x[1])).map(lambda x : (x[0],x[1][0]/x[1][1])).collectAsMap()

    #Model_file 
     
    train_model_map= sc.textFile(sys.argv[3]).map(lambda x :json.loads(x)).map(lambda  x: (tuple(sorted([user_dict[x["u1"]],user_dict[x["u2"]]])),x["sim"])).collectAsMap()

    #test_file
    text_file = sc.textFile(sys.argv[2]).map(lambda x : json.loads(x)).map(lambda x :(x['business_id'],x['user_id'])).map(lambda x : {"user_id":x[1],"business_id":x[0],"stars":getPredictedRatings1(x,business_dict,user_dict,businessUserRatingMap,businessUserMap,userAverageMap)}).filter(lambda x : x["stars"]>0).collect()

    f = open(sys.argv[4],"w")
    for value in text_file :
        json.dump(value,f)
        f.write("\n")
    f.close()
