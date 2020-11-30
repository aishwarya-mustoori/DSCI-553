from pyspark import SparkContext
import json 
import itertools 
import sys
import math
import time
import random 

def coeeff(x): 
    first_business = x[0]
    second_business = x[1]
    coeff =0 
    users_first = businessUserMap[first_business]
    users_second = businessUserMap[second_business]
    co_rated_users = set(users_first).intersection(set(users_second))
    co_rated_users_len = len(co_rated_users) 
    if(co_rated_users_len >= 3): 
        # The businesses are co rated : 
        corated = 0  
        sqrt_first = 0 
        sqrt_second = 0
        first_user_avg= 0
        second_user_avg = 0 
        for user in co_rated_users: 
            first_user_avg = first_user_avg + businessUserRatingMap[(first_business,user)]
            second_user_avg = second_user_avg + businessUserRatingMap[(second_business,user)]
        first_user_avg = first_user_avg/co_rated_users_len
        second_user_avg = second_user_avg/co_rated_users_len
        for user in co_rated_users : 
            business_rating_first = businessUserRatingMap[(first_business,user)]
            business_rating_second = businessUserRatingMap[(second_business,user)]
            corated = corated + (( business_rating_first-first_user_avg) * (business_rating_second-second_user_avg))
            sqrt_first = pow(business_rating_first-first_user_avg,2) + sqrt_first
            sqrt_second = pow(business_rating_second-second_user_avg,2)+sqrt_second
        if(corated>=0 and sqrt_second> 0 and sqrt_first > 0 ): 
            coeff = corated/(math.sqrt(sqrt_first)*math.sqrt(sqrt_second))
            return coeff
    return coeff
def minHashRows(row, m):
    minHashRows = list()
    allBusiness = row
    for number in range(len(hashFunctions_a)):
        minHashValue = sys.maxsize
        for column in allBusiness:
            # Calculating ax+b % m for each user
            hashValue = (
                hashFunctions_a[number]* business_dictionary[column] + hashFunctions_b[number]) % m
            minHashValue = min(hashValue, minHashValue)
        minHashRows.append(minHashValue)
    if(len(allBusiness) != 0):
        return minHashRows

def createCombinations(businesses): 
    all_pairs = []
    businesses = list(set(businesses))
    for i in range(0,len(businesses)): 
        for j in range(i+1,len(businesses)):
            coef = coeeff([businesses[i],businesses[j]])
            if(coef>0):
                all_pairs.append({"b1":businesses[i],"b2":businesses[j],"sim":coef})
    return all_pairs

def createCombinationsUser(users): 
    all_pairs = []
    users = list(set(users))
    for i in range(0,len(users)): 
        for j in range(i+1,len(users)):
            business1= businessUserMap[users[i]]
            business2 = businessUserMap[users[j]]
            sort = sorted([users[i],users[j]])
            jaccard = len(set(business1).intersection(set(business2)))/len(set(business1).union(set(business2)))
            if(jaccard>= 0.01):
                coef = coeeff([users[i],users[j]])
                if(coef>0):
                    all_pairs.append((sort[0],sort[1],coef))
    return list(all_pairs)


start = time.time()
sc = SparkContext().getOrCreate()

if(sys.argv[3] == "item_based"):
    input = sc.textFile(sys.argv[1]).map(lambda x : json.loads(x)).map(lambda x : ((x['business_id'],x['user_id']),x['stars']))
    businessUserRatingMap = input.collectAsMap()
    businessUserMap = input.map(lambda x:(x[0][0],[x[0][1]])).reduceByKey(lambda x,y : x+y).collectAsMap()
    distinct_business = input.map(lambda x: (1,[x[0][0]])).reduceByKey(lambda x,y : x+y).flatMap(lambda x: createCombinations(x[1]))

    f = open(sys.argv[2],"w")
    for values in distinct_business.collect():
        json.dump(values,f)
        f.write("\n")
    end = time.time()
    print("Duration"+str(end-start))
    f.close()
    sc.stop()
else : 
    
    input = sc.textFile(sys.argv[1]).map(lambda x : json.loads(x)).map(lambda x : ((x['user_id'],x['business_id']),x['stars']))
    businessUserRatingMap = input.collectAsMap()
    
    
    distinct_business = input.map(lambda x: x[0][1]).distinct().sortBy(lambda x: x[0]).collect()
    # 30 Hash Functions
    n = 35
    # Generating random a and b values for hash functions
    hashFunctions_a = random.sample(range(10000),n)
    hashFunctions_b = random.sample(range(10000),n)
    # As the threshold is 0.055, the (1/b)^1/r value that satisfies for n = 100 is b = 100 and r = 1(s = 0.01)
    # b = 100
    # r = 1
    # Create user index for each distinct user,
    business_dictionary = {}
    index = 0
    for user in distinct_business:
        business_dictionary[user] = index
        index = index + 1
    m = 2454587

    for_original_jaccard = input.map(lambda x:(x[0][0],[x[0][1]])).reduceByKey(lambda x,y : x+y)
    data = for_original_jaccard.map(lambda x:(x[0],minHashRows(x[1], m)))
    businessUserMap = for_original_jaccard.collectAsMap()
    
    distinct_users = data.flatMap(lambda x:[((i,list(x[1])[i]),[x[0]]) for i in range(0,len(x[1]))]).reduceByKey(lambda x,y : x+y).filter(lambda x : len(set(x[1]))>1).flatMap(lambda x : tuple(createCombinationsUser(x[1]))).distinct().map(lambda x : {"u1" : x[0] ,"u2":x[1], "sim":x[2]})
    f = open(sys.argv[2],"w")
    for values in distinct_users.collect():
        json.dump(values,f)
        f.write("\n")
    end = time.time()
    print("Duration "+str(end-start))
    f.close()
    sc.stop()



    



