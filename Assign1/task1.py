from pyspark import SparkContext
import json
import datetime
import sys


def removePunctuation(key) : 
    excluded = ['(', '[', ',', '.', '!', '?', ':', ';' , ']' ,')','']
    for i in excluded : 
        if i in key : 
            key = key.replace(i,"")
    
    return key

sc1 = SparkContext.getOrCreate()
review_json = sc1.textFile(sys.argv[1]).map(lambda k: json.loads(k)) 

#Year
year = sys.argv[4]
years = review_json.filter(lambda x : int(datetime.datetime.strptime(x['date'], '%Y-%m-%d %H:%M:%S').year) == int(year))


#businesses : 
business_id = review_json.map(lambda x: x['business_id'])

#Top Users
top_users = int(sys.argv[5])

user_ids = review_json.map(lambda x: (x['user_id'],1))
reduced  = user_ids.reduceByKey(lambda x,y : x+y)
sort = reduced.takeOrdered(top_users,lambda x :( -x[1],x[0]))
takeTopM = sort


# 5th 
top_n = int(sys.argv[6])
excluded = ['(', '[', ',', '.', '!', '?', ':', ';' , ']' ,')','']
file1 = open(sys.argv[3],"r")
x  = file1.readline()
while(x != "") : 
    excluded.append(x.lower().strip())
    x = file1.readline()
text = review_json.map(lambda x: x['text'])
map1 = text.flatMap(lambda text: text.lower().split(' '))
filter1 = map1.map(removePunctuation).filter(lambda x : x != None and x!="" and x.lower() not in excluded )
stop_map = filter1.map(lambda x :(x,1))
stop_reduce = stop_map.reduceByKey(lambda x,y : x+y)
stop_sort = stop_reduce.sortBy(lambda x : (-x[1],x[0])).map(lambda x :x[0]).take(top_n)
print(stop_sort)
ans = {
    "A": review_json.count(),
    "B": years.count(),
    "C": business_id.distinct().count(), 
    "D": takeTopM,
    "E": stop_sort,
}
f = open(sys.argv[2],"w")
json.dump(ans, f)
f.close()
sc1.stop()

