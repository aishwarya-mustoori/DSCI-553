from pyspark import SparkContext
import json
from collections import defaultdict 
import sys 

top_n = int(sys.argv[5])

if sys.argv[4] == "spark": 
    sc1 = SparkContext.getOrCreate()
    review_data = sc1.textFile(sys.argv[1]).map(lambda k: json.loads(k))
    business_data = sc1.textFile(sys.argv[2]).map(lambda k: json.loads(k))
    business_json = business_data.filter(lambda x : x['categories'] is not None).flatMap(lambda x: [(x['business_id'], y) for y in x['categories'].split(", ")]) 
    review_json = review_data.map(lambda x: (x['business_id'],x['stars']))

    join = review_json.leftOuterJoin(business_json)
    reduce = join.map(lambda x :(x[1][1],(x[1][0],1))).reduceByKey(lambda x,y : (x[0]+y[0], x[1]+y[1],2)).map(lambda x : (x[0],float(x[1][0]/x[1][1]))).sortBy(lambda x : (-x[1],x[0]))

    result = {
        "result" : reduce.take(top_n)
    }
    f = open(sys.argv[3],"w")
    json.dump(result, f)
    f.close()
    sc1.stop()

else : 
    print(sys.argv[4])
    f = open(sys.argv[1])
    lines = f.readlines()
    review_list=dict()
    for line in lines: 
        review_data = json.loads(line)
        if(review_list.get(review_data['business_id']) == None) :
            review_list[review_data['business_id']]= [review_data['stars'],1]
        else : 
            review_list[review_data['business_id']] = [review_list[review_data['business_id']][0]+ review_data['stars'], 1+review_list[review_data['business_id']][1]]
        

    f = open(sys.argv[2])
    lines = f.readlines()
    business_list=[]
    category = defaultdict(list)
    for line in lines:
        business_data = json.loads(line)
        if(business_data['categories']!= None) :
            categories = business_data['categories'].split(", ")
            for i in categories: 
                category[i].append(business_data['business_id'])
    avg_dict = defaultdict()
    for category,business_ids in category.items() :
        sum = 0 
        count = 0
        for id in business_ids : 
            if(review_list.get(id) !=None) : 
                    sum = sum + review_list[id][0]
                    count = count + review_list[id][1]  
        if(count != 0):
            avg_dict[category] = sum/count

    ans_dict = sorted(avg_dict.items(), key = lambda x : (-x[1],x[0]))
    result = {
        "result" : ans_dict[:top_n]
    }
    f = open(sys.argv[3],"w")
    json.dump(result, f)
    f.close()



