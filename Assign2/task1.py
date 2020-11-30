from pyspark import SparkContext
import sys
import itertools
import time
import math

def aprioriAlgo(partion,support) : 
    # Apriori Algorithm
    # To Find First Frequent Pairs , count the number of values, for all count >= support, consider them to be L1
    ans = []
    index = 0 
    dictionaryForC1 = dict()
    for items in partion :
        for item in items :
            item_1 = item
            if(dictionaryForC1.get(item_1) != None) :
                dictionaryForC1[item_1] += 1
            else : 
                dictionaryForC1[item_1] = 1
        index = index + 1
    support = math.ceil(support * (index/count))
    if(len(dictionaryForC1)>0) :
        l1 = [tuple([key]) for key,value  in dictionaryForC1.items() if value >= support]
        l1= sorted(l1)
        ans = ans + l1
        # for All remaining pairs, rule of monotonicity.
        #If [a,b,c] is a frequent pair, then its immidiate subsets [a,b],[b,c],[c,a] should also be frequent.
        min_k = 2
        frequent_item_subset = l1
        while(True) :
            Cn= list()
            for frequent_item in itertools.combinations(frequent_item_subset,2) : 
                candidateList = list(frequent_item[0])+ list(frequent_item[1])
                candidateSet = set(candidateList)
                candidateSet = sorted(candidateSet)
                if(len(candidateSet) == min_k and tuple(candidateSet) not in Cn): 
                    #Generate its subset, check if all of them are frequent or not.
                    flag = True
                    if(flag) : 
                        count_value = 0 
                        for backets in partion :
                            if(set(candidateSet).issubset(set(backets))) :
                                count_value = count_value + 1
                        if(count_value >=support) : 
                            Cn.append(tuple(candidateSet))           
            Cn.sort()
            if(len(Cn)==0) : 
                break
            ans = ans + Cn
            frequent_item_subset = Cn
            min_k = min_k+1
    return ans
def mapPhase2(partion,candidatePairs):
    #Map Phase 2 
    dictionary = {}
    for basket in partion : 
        for pair in candidatePairs : 
            if(set(pair).issubset(set(basket))) : 
                if(dictionary.get(tuple(pair)) != None) : 
                    dictionary[tuple(pair)] +=1
                else : 
                    dictionary[tuple(pair)] =1
    ans = [tuple([key,value]) for key,value in dictionary.items()]
    return ans


startDate = time.time()
sc = SparkContext().getOrCreate()
csv_file = sc.textFile(sys.argv[3])
main = csv_file.take(1)
rddcsv = csv_file.filter(lambda x: x not in str(main).strip("[]"))
rddMap = rddcsv.map(lambda x :x.split(","))
if int(sys.argv[1].strip()) == 1:
    caseRdd = rddMap.map(lambda x: (x[0],x[1])).reduceByKey(lambda x,y: x +", "+ y)
else : 
    caseRdd = rddMap.map(lambda x: (x[1],x[0])).reduceByKey(lambda x,y: x +", "+ y)
caseRdd = caseRdd.map(lambda x : x[1].split(", "))
count = caseRdd.count()
support = int(sys.argv[2])
candidatePairs = caseRdd.mapPartitions(lambda x : aprioriAlgo(list(x),support)).distinct().sortBy(lambda x : (len(set(x)),x))
collect = candidatePairs.collect()
frequentPairs = caseRdd.mapPartitions(lambda x : mapPhase2(list(x),collect)).reduceByKey(lambda x,y : x+y).filter(lambda x :x[1]>=support).sortBy(lambda x : (len(set(x[0])),x[0])).map(lambda x : x[0])

f = open(sys.argv[4],"w")
f.write("Candidates:\n")
index = 1
start = 0
for i in collect : 
    if(len(i)==index and index !=1) :
        f.write(","+str(i))
    elif(len(i)==index and index == 1): 
        if(start == 0) : 
            f.write("('"+str(i[0]) +"')")
        else : 
            f.write(",('"+str(i[0]) +"')")
        start = 1
    elif(len(i)==index + 1):
        index = index + 1
        f.write('\n')
        f.write('\n')
        f.write(str(i))

f.write('\n')
f.write('\n')
f.write("Frequent Itemsets:\n")
start = 0 
index = 1
for i in frequentPairs.collect() : 
    if(len(i)==index and index !=1) :
        f.write(","+str(i))
    elif(len(i)==index and index == 1): 
        if(start == 0) : 
            f.write("('"+str(i[0]) +"')")
        else : 
            f.write(",('"+str(i[0]) +"')")
        start = 1
    elif(len(i)==index + 1):
        index = index + 1
        f.write('\n')
        f.write('\n')
        f.write(str(i))
print("Duration: "+  str(time.time()-startDate))

f.close()
sc.stop()

