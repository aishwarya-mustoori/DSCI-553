from pyspark import SparkContext
import json
import random
import itertools
import sys


def minHashRows(row, m):
    minHashRows = list()
    allUsers = row
    for number in range(len(hashFunctions_a)):
        minHashValue = sys.maxsize
        for column in allUsers:
            # Calculating ax+b % m for each user
            hashValue = (
                hashFunctions_a[number]* user_dictionary[column] + hashFunctions_b[number]) % m
            minHashValue = min(hashValue, minHashValue)
        minHashRows.append(minHashValue)
    if(len(allUsers) != 0):
        return minHashRows


sc = SparkContext().getOrCreate()
input = sc.textFile(sys.argv[1]).map(lambda x: json.loads(x))

distinct_users = input.map(lambda x: x['user_id']).distinct().sortBy(
    lambda x: x[0]).collect()
# 100 Hash Functions
n = 100
# Generating random a and b values for hash functions
hashFunctions_a = random.sample(range(10000),100)
hashFunctions_b = random.sample(range(10000),100)
# As the threshold is 0.055, the (1/b)^1/r value that satisfies for n = 100 is b = 100 and r = 1(s = 0.01)
# b = 100
# r = 1
# Create user index for each distinct user,
user_dictionary = {}
index = 0
for user in distinct_users:
    user_dictionary[user] = index
    index = index + 1
m = 2454587

for_original_jaccard = input.map(lambda x: (x['business_id'], x['user_id'])).reduceByKey(lambda x, y: x+", "+y).sortBy(lambda x: x[0]).map(lambda x : (x[0],x[1].split(", ")))
data = for_original_jaccard.map(lambda x:(x[0],minHashRows(x[1], m)))
map1 = for_original_jaccard.collectAsMap()

values = data.flatMap(lambda x:[((i,list(x[1])[i]),[x[0]]) for i in range(0,len(x[1]))]).reduceByKey(lambda x,y : x+y).filter(lambda x : len(set(x[1]))>1).map(lambda x : (x[0],sorted(x[1]))).flatMap(lambda x : list(itertools.combinations(x[1],2))).distinct().sortBy(lambda x : (x[0],x[1])).map(lambda x: {"b1":x[0],"b2":x[1], "sim":len(set(map1[x[0]]).intersection(set(map1[x[1]])))/len(set(map1[x[0]]).union(set(map1[x[1]])))}).filter(lambda x: x["sim"] >= 0.055)

# Combinations cannot be done with having values.
# main_ans = values.flatMap(lambda x : list(itertools.combinations(x[1],2)))
f = open(sys.argv[2], "w")
for value in values.collect():
    json.dump(value, f)
    f.write("\n")
f.close()
sc.stop()

