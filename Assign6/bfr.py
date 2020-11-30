from collections import defaultdict
import random
from pyspark import SparkContext
import sys
import math
import json
import time
import os


class KMeans:
    def __init__(self):
        self.centroids = defaultdict(list)
        self.noOfClusters = 0
        self.centroidPoints = list()
        self.centroidStats = defaultdict(list)
        self.newCentroids = defaultdict(list)

    def init(self, no_of_clusters):
        self.centroids = defaultdict(list)
        self.noOfClusters = no_of_clusters
        self.centroidPoints = list()
        self.newCentroids = defaultdict(list)

    def selectRandomCentroids(self, data):
        # random.seed(800)
        if(len(data) != 0):
            number = len(data)
            if(self.noOfClusters > len(data)):
                self.noOfClusters = number
            rand_nos = random.sample(range(number), self.noOfClusters)
        
            for numbers in rand_nos:
                self.centroidPoints.append(data[numbers])
            
            

    def calculateDistance(self, centroid, point):
        sum = 0

        for i in range(len(point)):
            sum += pow((float(point[i])-float(centroid[i])), 2)
        sum = math.sqrt(sum)
        return sum

    def kmeans(self, max_iter, dataPoints):
        index = 0

        while(max_iter > 0):

            self.centroids = defaultdict(list)
            for point in dataPoints:
                distance = float('inf')
                updatecentroid = list()
                for centroid in self.centroidPoints:
                    d = self.calculateDistance(centroid, point)
                    if(distance > d):
                        distance = d
                        updatecentroid = centroid
                self.centroids[tuple(updatecentroid)] += [point]

            # update the centroids
            oldcentroids = list(self.centroidPoints)
            self.centroidPoints = list()
            newCentroids = defaultdict(list)
            for centroid, points in self.centroids.items():
                centroid_sum = defaultdict(int)
                n = len(points)
                for point in points:
                    sum = 0
                    for dimen in range(len(point)):
                        centroid_sum[dimen] += float(point[dimen])
                for k, v in centroid_sum.items():
                    centroid_sum[k] = v/n
                newCentroids[tuple(centroid_sum.values())] = points

                self.centroidPoints.append(list(centroid_sum.values()))
            self.newCentroids = newCentroids
            if(sorted(oldcentroids) == sorted(self.centroidPoints)):
                break

            max_iter -= 1


def calculateMDistance(point, n, sum, sq):
    # Calculate distance
    distance = 0
    for i in range(len(point)):
        ci = sum[i]/n
        pi = float(point[i])

        variance = abs(((sq[i]/n)-(pow((sum[i]/n), 2))))

        if(variance != 0):
            distance += ((pi-ci)*(pi-ci))/variance

    return math.sqrt(distance)


def calculateEDistance(centroid, point):
    sum = 0
    for i in range(len(point)):
        sum += pow((float(point[i])-float(centroid[i])), 2)
    sum = math.sqrt(sum)
    return sum


def mergeIntoAnySet(point, n, sum, sq,dict1):
        not_added_values = []
        distance = float('inf')
        addingCentroid = tuple([100,100,100,100])
        for centroid in n.keys():
            mdistance = calculateMDistance(
                point, n[centroid], sum[centroid], sq[centroid])
         
            if(mdistance <= 3*math.sqrt(len(point))):
                if(distance > mdistance):
                    distance = mdistance
                    addingCentroid = centroid 
        return [addingCentroid,point]



def updateStats(points, n, sum, sq,ds_dict,centroid):
    n1 = int(n[centroid])
    sum1 = list(sum[centroid])
    sq1 = list(sq[centroid])
    newCentroid = []
    old_points = list(ds_dict[tuple(centroid)])
    for point in points : 
        n1+=1
        for dimen in range(len(point)):
            sum1[dimen] += float(point[dimen])
            sq1[dimen] += (float(point[dimen])) * (float(point[dimen]))
        old_points.append(dataPointMap[tuple(point)])
    n.pop(centroid)
    sum.pop(centroid)
    sq.pop(centroid)

    for i in range(len(sum1)):
            newCentroid.append(sum1[i]/n1)
    
    
    n[tuple(newCentroid)] = n1
    sum[tuple(newCentroid)] = sum1
    sq[tuple(newCentroid)] = sq1
    ds_dict[tuple(newCentroid)] = old_points
    ds_dict.pop(tuple(centroid))
    
    return n, sum, sq,ds_dict


def mergeCS(n, sum, sq,cs_dict):
    not_added_values = []
    addingCentroid = []
    main_add = defaultdict(list)
    
    for cen1 in n.keys():
        distance = float('inf')
        for centroid in n.keys():
            mdistance = calculateMDistance(
                cen1, n[centroid], sum[centroid], sq[centroid])
            if(mdistance <= 3*math.sqrt(len(cen1))):
                if(distance > mdistance):
                    distance = mdistance
                    addingCentroid = centroid
        if(addingCentroid != cen1 and len(addingCentroid) !=0):
            main_add[addingCentroid].append(cen1)
    return n, sum, sq,cs_dict,main_add


def update2Stats(centroid, n, sum, sq, points ,cs_dict):
    newCentroid = []
    sum1 = list(sum[centroid])
    sq1 = list(sq[centroid])
    n1 = int(n[centroid])
    old_points = list(cs_dict[tuple(centroid)])
    for point in points : 
        n1 +=  int(n[point])
        for i in range(len(sum1)):
            sum1[i] += float(sum[point][i])
            sq1[i] += float(sq[point][i])
        old_points +=list(cs_dict[tuple(point)])

    for i in range(len(sum1)):
        newCentroid.append(sum1[i]/n1)

    n.pop(centroid)
    sum.pop(centroid)
    sq.pop(centroid)
    
    for point in points:
        n.pop(point)
        sum.pop(point)
        sq.pop(point)
        cs_dict.pop(tuple(point))


    n[tuple(newCentroid)] = n1
    sum[tuple(newCentroid)] = sum1
    sq[tuple(newCentroid)] = sq1
    
    cs_dict.pop(tuple(centroid))
    
    cs_dict[tuple(newCentroid)] = old_points
    return n, sum, sq,cs_dict


def mergeAllCS(discard_n, discard_set_sum, discard_set_squares, xyz, cs_sum, cs_squares,ds_dict,cs_dict):
    cs_n = list(xyz.keys())
    for cscentroid in cs_n:
        distance = float('inf')
        change = []
        values = discard_n.keys()
        for dscentroid in values:
            d = calculateMDistance(
                cscentroid, discard_n[dscentroid],discard_set_sum[dscentroid],discard_set_squares[dscentroid])
            if(distance > d):
                distance = d
                change = dscentroid

        discard_n, discard_set_sum, discard_set_squares,ds_dict,cs_dict = update3Stats(
            change, discard_n, discard_set_sum, discard_set_squares, xyz[cscentroid], cs_sum[cscentroid], cs_squares[cscentroid], cscentroid,ds_dict,cs_dict)

    return discard_n, discard_set_sum, discard_set_squares,ds_dict,cs_dict


def mergeAllRS(discard_n, discard_set_sum, discard_set_squares, rs,ds_dict):
    for rscentroid in rs:

        distance = float('inf')
        change = []

        for dscentroid in discard_n.keys():

            d = calculateMDistance(rscentroid,discard_n[dscentroid],discard_set_sum[dscentroid],discard_set_squares[dscentroid])
            if(distance > d):
                distance = d
                change = dscentroid
        squares = []
        for i in change:
            squares.append(i*i)
        dictionary = {}
        dictionary[tuple(rscentroid)] = [dataPointMap[tuple(rscentroid)]]
        discard_n, discard_set_sum, discard_set_squares,ds_dict,x = update3Stats(tuple(
            change), discard_n, discard_set_sum, discard_set_squares, 1, rscentroid, squares, tuple(rscentroid),ds_dict,dictionary)
    return discard_n, discard_set_sum, discard_set_squares,ds_dict


def update3Stats(centroid, n, sum, sq, cs_n, cs_sum, cs_sq, point,ds_dict,cs_dict):
    newCentroid = []
    n1 = n[centroid] + cs_n
    sum1 = list(sum[centroid])
    sq1 = list(sq[centroid])
    for i in range(len(sum1)):
        sum1[i] += float(cs_sum[i])
        sq1[i] += float(cs_sq[i])

    for i in range(len(sum1)):
        newCentroid.append(sum1[i]/n1)
    
    n.pop(centroid)
    sum.pop(centroid)
    sq.pop(centroid)

    n[tuple(newCentroid)] = n1
    sum[tuple(newCentroid)] = sum1
    sq[tuple(newCentroid)] = sq1


    old_points = list(ds_dict[tuple(
        centroid)])+list(cs_dict[tuple(point)])
    if(cs_dict.get(tuple(point)) != None):
        cs_dict.pop(tuple(point))
    ds_dict.pop(tuple(centroid))
    ds_dict[tuple(newCentroid)] = old_points
    
    return n, sum, sq,ds_dict,cs_dict


def stats(centroids,ds_dict):
    discard_sum = {}
    discard_sq = {}
    discard_n = {}
    for centroid, points in centroids:
        
        n = len(points)
        
        sum = defaultdict(int)
        sumsq = defaultdict(int)
        discard_n[centroid] = n

        pointsD = []
        for point in points:
            pointsD.append(dataPointMap[tuple(point)])
            for dimen in range(len(point)):
                sum[dimen] += float(point[dimen])
                sumsq[dimen] += (float(point[dimen])) * (float(point[dimen]))
        discard_set_sum[centroid] = list(sum.values())
        discard_set_squares[centroid] = list(sumsq.values())
        ds_dict[centroid]  = pointsD

    return discard_n, discard_set_sum, discard_set_squares,ds_dict

start1 = time.time()
sc = SparkContext().getOrCreate()
sc.setLogLevel("WARN")
no_of_clusters = int(sys.argv[2])
obj = KMeans()
ds_dict = {}
cs_dict = {}
allFiles = os.listdir(sys.argv[1]+"/")
index = 0
rs =[]
discard_set_sum = {}
discard_set_squares = {}
discard_n = {}
CS_points = []
RS_points = []
cs_set_sum = {}
cs_set_squares = {}
cs_n = {}
fresh_rs = []
all_results =[]
remaining_cs_all = []
dataPointMap = {}
yindex = 0
for files in allFiles:
    dataPoints = sc.textFile(sys.argv[1]+"/"+files).flatMap(lambda x: x.split(
        "\n")).map(lambda x: (x.split(",")[0], x.split(",")[1:]))
    
    dataPointMap.update(dataPoints.map(lambda x: (tuple(x[1]), x[0])).collectAsMap())
    flag = False
    start = int(dataPoints.take(1)[0][0])
    total = dataPoints.count()
    index += 1
    print(total)
    if(index == 1):
        # Initialization of data points
        dataInit = dataPoints.filter(lambda  x: int(x[0])<= int(start+(total/10))).map(lambda x: x[1]).collect()
        

        # Run K- Means now on discard sets

        obj2 = KMeans()
        obj2.init(no_of_clusters)
        obj2.selectRandomCentroids(dataInit)
        obj2.kmeans(10, dataInit)

        # Discard Set Found so set the N,SUM,SUMSQ For each dimension

        discard_n, discard_set_sum, discard_set_squares,ds_dict = stats(
            obj2.newCentroids.items(),ds_dict)
        print("ds1")
        print(len(discard_n))

        # Find RS AND CS
        outliers = dataPoints.filter(lambda  x: int(x[0])> int(start+(total/10))).map(lambda x: x[1]).collect()
        obj3 = KMeans()
        obj3.init(3*no_of_clusters)
        obj3.selectRandomCentroids(outliers)
        obj3.kmeans(5, outliers)
        print("done outliers")

        for centroid, points in obj3.newCentroids.items():
            if(len(points) ==0 ):
                continue
            if(len(points) == 1):
                for point in points:
                    RS_points.append(point)
            else:
                CS_points.append([centroid, points])

        # Find statistics of CS
        print("yaas")
        cs_n, cs_set_sum, cs_set_squares,cs_dict = stats(CS_points,cs_dict)
        
        fresh_rs = RS_points
        print(len(cs_dict))
        print("RS_points out" + str(len(RS_points)))
        ds= 0
        for k,v in discard_n.items():
            ds += float(v)

        cs = 0 
        for k,v in cs_n.items():
            cs += float(v)
        print([index-1, len(discard_n), ds, len(cs_n),cs, len(fresh_rs)])
        all_results.append([index-1, len(discard_n), ds, len(cs_n),cs, len(RS_points)])
    
    else:
        
        # while(w<= start + total):
        #     print("index"+str(index))
                
                
        # ACTUAL BFR Algorithm to remaining data
        # Add the ones that can be added in DS
        
        dataRemaining = dataPoints.map(lambda x: x[1]).map(lambda x : mergeIntoAnySet(x, discard_n, discard_set_sum, discard_set_squares,ds_dict)).map(lambda x : (x[0],[x[1]])).reduceByKey(lambda x,y : x+y).collectAsMap()
        
        for centroid,points in dataRemaining.items():
            
            if(centroid== tuple([100,100,100,100])):
                allrs = sc.parallelize(points).map(lambda x : mergeIntoAnySet(x, cs_n, cs_set_sum, cs_set_squares,cs_dict)).map(lambda x : (x[0],[x[1]])).reduceByKey(lambda x,y : x+y).collectAsMap()
                for centroid,points in allrs.items() :
                    if(centroid == tuple([100,100,100,100])):
                        
                        rs = points
                    else:
                        cs_n, cs_set_sum, cs_set_squares,cs_dict = updateStats(points,cs_n, cs_set_sum, cs_set_squares,cs_dict,centroid)  
            
            else : 
                discard_n, discard_set_sum, discard_set_squares,ds_dict = updateStats(points,discard_n, discard_set_sum, discard_set_squares,ds_dict,centroid)
        
        # Add ones that cannot be into CS into RS
            
        print("rs"+str(len(rs)))
        
        for point in rs:
            fresh_rs.append(point)

        # Run k means on rs :
        if(len(rs)> 3*no_of_clusters):
            obj4 = KMeans()
            obj4.init(3*no_of_clusters)
            obj4.selectRandomCentroids(fresh_rs)
            obj4.kmeans(3, fresh_rs)

                # Combine these to nearby CS if not a single point
            fresh_rs = []
            for centroid, points in obj4.newCentroids.items():
                        if(len(points) ==0 ):
                            
                            continue
                        if(len(points) ==1):
                            fresh_rs.append(points[0])
                        else:
                            # Update CS Stats
                            n = len(points)
                            sum = defaultdict(int)
                            sumsq = defaultdict(int)
                            cs_n[centroid] = n
                            
                            pointD = []
                            for point in points:
                                for dimen in range(len(point)):
                                    sum[dimen] += float(point[dimen])
                                    sumsq[dimen] += (float(point[dimen])) * (float(point[dimen]))
                                pointD.append(dataPointMap[tuple(point)])
                            cs_set_sum[centroid] = list(sum.values())
                            cs_set_squares[centroid] = list(sumsq.values())
                            cs_dict[centroid]  = pointD
                            flag = True
        
        if(flag):
            cs_n, cs_set_sum, cs_set_squares,cs_dict,update_cs = mergeCS(
                                    cs_n, cs_set_sum, cs_set_squares,cs_dict)
            for addingCentroid,points in update_cs.items():
                cs_n, cs_set_sum, cs_set_squares,cs_dict = update2Stats(addingCentroid,cs_n, cs_set_sum, cs_set_squares, points,cs_dict)
        

    
        ds= 0
        for k,v in ds_dict.items():
            ds += int(len(v))

        cs = 0 
        for k,v in cs_dict.items():
            cs += int(len(v))
        print([index-1, len(ds_dict), ds, len(cs_dict),cs, len(fresh_rs)])
        all_results.append([index-1, len(ds_dict), ds, len(cs_dict),cs, len(fresh_rs)])
    
    

# Add remaining cs clusters and rs clusters to nearest DS :
discard_n, discard_set_sum, discard_set_squares,ds_dict,cs_dict = mergeAllCS(
                discard_n, discard_set_sum, discard_set_squares, cs_n, cs_set_sum, cs_set_squares,ds_dict,cs_dict )

discard_n, discard_set_sum, discard_set_squares,ds_dict= mergeAllRS(
                discard_n, discard_set_sum, discard_set_squares, fresh_rs,ds_dict)
fresh_rs = [] 
cs_n = {}
print(len(ds_dict))
print(len(cs_dict))

# ds= 0
# for k,v in ds_dict.items():
#     ds += int(len(v))

# cs = 0 
# for k,v in cs_dict.items():
#     cs += int(len(v))
# all_results.append([index-1, len(ds_dict), ds, len(cs_dict),cs, len(fresh_rs)])

f = open(sys.argv[4], "w")
f.write("round_id,nof_cluster_discard,nof_point_discard,nof_cluster_compression,nof_point_compression,nof_point_retained")
f.write("\n")
for results in all_results:
    print(results)
    f.write(str(results).strip("[]").replace(" ",""))
    f.write("\n")
f.close()
f = open(sys.argv[3],"w")
cluster_index = 0 
cluster = {}
for centroid,points in ds_dict.items() : 
    for point in points : 
        cluster[str(point)] = cluster_index
    cluster_index +=1
    
    
json.dump(cluster,f)
f.close()
print("Duration"+str(time.time()-start1))
sc.stop()




