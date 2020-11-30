from collections import defaultdict
import random
from pyspark import SparkContext
import sys
import math
import json
class KMeans : 
    def __init__(self) : 
        self.centroids = defaultdict(list)
        self.noOfClusters = 0
        self.centroidPoints = list()
        self.centroidStats = defaultdict(list)


    def init(self,no_of_clusters):
        self.centroids = defaultdict(list)
        self.noOfClusters = no_of_clusters
        self.centroidPoints = list()

    def selectRandomCentroids(self,data): 
        if(len(data) !=0):
            number = len(data)
            rand_nos = random.sample(range(number),self.noOfClusters)
            for numbers in rand_nos:
                self.centroidPoints.append(data[numbers])
            # print(self.centroidPoints)
            self.centroidPoints = sorted(self.centroidPoints)
    
    def calculateDistance(self,centroid,point):
        sum = 0
        for i in range(len(point)):
            sum += pow((float(point[i])-float(centroid[i])),2)
        sum = math.sqrt(sum)
        return sum

    def kmeans(self,max_iter,dataPoints):
        index = 0 
        while(True):
            
            self.centroids = defaultdict(list)
            for point in dataPoints : 
                distance = float('inf')
                updatecentroid = list()
                for centroid in self.centroidPoints : 
                    d = self.calculateDistance(centroid,point)
                    if(distance>d):
                        distance = d
                        updatecentroid = centroid
                self.centroids[tuple(updatecentroid)] +=[point]

            
            #update the centroids 
            oldcentroids = list(self.centroidPoints)
            self.centroidPoints = list()
            
            for centroid,points in self.centroids.items(): 
                centroid_sum = defaultdict(int)
                n = len(points)
                for point in points :
                    sum = 0 
                    for dimen in range(len(point)) : 
                        centroid_sum[dimen]+= float(point[dimen])
                for k,v in centroid_sum.items():
                    centroid_sum[k] = v/n
                self.centroidPoints.append(list(centroid_sum.values()))
            if(sorted(oldcentroids) == sorted(self.centroidPoints)):
                break
            
            max_iter -=1
        cluster_index = 0
        pointCluster = defaultdict(int)
        sort = sorted(self.centroids.items())
        for [centroid,points] in sort:
            for point in points:
                
                pointCluster[dataPointMap[tuple(point)]]= cluster_index
            cluster_index +=1
            
        return pointCluster





sc = SparkContext().getOrCreate()
no_of_clusters  = int(sys.argv[2])
obj = KMeans()
dataPoints = sc.textFile(sys.argv[1]).flatMap(lambda x : x.split("\n")).map(lambda x : (x.split(",")[0],x.split(",")[1:]))
dataPointMap = dataPoints.map(lambda x :(tuple(x[1]),x[0])).collectAsMap()
dataPoints = dataPoints.map(lambda x : x[1]).collect()
obj.init(no_of_clusters)
obj.selectRandomCentroids(dataPoints)
pointCluster = obj.kmeans(10000,dataPoints)


f = open(sys.argv[3],"w")
json.dump(pointCluster,f)
f.close()





        
        

        



            



