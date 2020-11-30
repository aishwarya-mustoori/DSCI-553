from pyspark import SparkContext
from pyspark import SparkConf
import sys
import itertools
import time
import json
from collections import defaultdict

def bfs(root, adjacencyMatrixMap, visited):
    visited = visited + [root]
    q = [root]
    ans = [root]
    edges = []
    while(len(q) != 0):
        root = q.pop()
        neighboours = adjacencyMatrixMap[root]
        for neigh in neighboours:
            if neigh not in visited:
                visited.append(neigh)
                q.append(neigh)
                ans.append(neigh)
    return ans


def all_communities(adjacencyMatrixMap):
    keys = adjacencyMatrixMap.keys()
    visited = []
    all = list()
    for key in keys:
        if(key not in visited):
            component = list()
            component = bfs(key, adjacencyMatrixMap, visited)
            visited = visited + component
            all = all + [sorted(component)]
    return all


def modularityCase1(component):
    modularity = 0
    vertices = component
    for i in range(len(vertices)):
        for j in range(0, len(vertices)):
            if([vertices[i], vertices[j]] in edges):
                Aij = 1
            else:
                Aij = 0
            modularity = modularity + \
                (Aij - ((degree[vertices[i]]*degree[vertices[j]])/(m)))

    return modularity

def modularityCase2(component):
    modularity = 0
    vertices = component
    for i in range(len(vertices)):
        for j in range(0, len(vertices)):
            if([vertices[i], vertices[j]] in edges):
                Aij = 1
            else:
                Aij = 0
            modularity = modularity + \
                (Aij - ((degree[vertices[i]]*degree[vertices[j]])/(2*m)))
    return modularity

def betweeness1(x):
    root = x
    level = defaultdict(list)
    level[0].append(root)
    shortestPath = {}
    parentNode = defaultdict(set)
    childNode = defaultdict(set)
    q = [root]
    visited = [root]
    shortestPath[root] = 1
    levelNode = {root: 0}
    while(len(q) != 0):
        q_root = q.pop(0)
        for neigh in adjacencyMatrixMap[q_root]:
            if(neigh not in visited):
                shortestPath[neigh] = shortestPath[q_root]
                childNode[q_root].add(neigh)
                parentNode[neigh].add(q_root)
                levelNode[neigh] = levelNode[q_root]+1
                level[levelNode[q_root]+1].append(neigh)
                q.append(neigh)
                visited.append(neigh)
            else:
                # Same level
                if(levelNode[q_root]+1 == levelNode[neigh]):
                    shortestPath[neigh] += shortestPath[q_root]
                    childNode[q_root].add(neigh)
                    parentNode[neigh].add(q_root)

    credit = {}
    edgeCosts = {}

    # First come the leaf nodes
    l = len(level)
    while(l >= 0):
        nodes = level[l-1]
        l = l-1
        for node in nodes:
            children = childNode[node]
            credit[node] = 1
            for child in children:
                credit[node] += edgeCosts[tuple(sorted([node, child]))]
            parents = list(parentNode[node])
            # Calculate for parents
            for parent in parents:
                edgeCosts[tuple(sorted([node, parent]))] = credit[node] * \
                    (shortestPath[parent]/shortestPath[node])
    ans = ((k, v) for k, v in edgeCosts.items())
    return ans

start = time.time()
conf = SparkConf()
conf.set("spark.executor.memory", "4g")
conf.set("spark.driver.memory", "4g")
sc = SparkContext('local[3]').getOrCreate(conf)

sc.setLogLevel("WARN")

if(sys.argv[1] == '1'):
    sample_text = sc.textFile(sys.argv[2]).map(lambda x: tuple(x.split(',')))
    sample_first = sample_text.take(1)

    edges = sample_text.filter(lambda x: x != sample_first[0]).flatMap(
        lambda x: ((x[0], x[1]), (x[1], x[0])))

elif(sys.argv[1] == '2'):
    sample_text = sc.textFile(sys.argv[2]).map(lambda x: x.split(','))
    sample_first = sample_text.take(1)
    distinct_users = sample_text.filter(lambda x: x != sample_first[0])

    sample_text = distinct_users.map(lambda x: (x[0], x[1]))
    stateUserMap = sample_text.map(lambda x: (x[0], [x[1]])).reduceByKey(
        lambda x, y: x+y).collectAsMap()

    jaccard_users = sample_text.map(lambda x: (x[1], x[0]))
    edges = jaccard_users.join(jaccard_users).filter(lambda x: len(set(x[1])) != 1).map(lambda x: (x[1][0], x[1][1], len(set(stateUserMap[x[1][0]]).intersection(set(
        stateUserMap[x[1][1]])))/len(set(stateUserMap[x[1][0]]).union(set(stateUserMap[x[1][1]]))))).filter(lambda x: x[2] >= 0.5).map(lambda x: tuple([x[0], (x[1])])).distinct()

adjacencyMatrix = edges.map(lambda x: (
    x[0], [x[1]])).reduceByKey(lambda x, y: x+y)
adjacencyMatrixMap = adjacencyMatrix.map(lambda x : (x[0],list(set(x[1])))).collectAsMap()
degree = adjacencyMatrix.map(lambda x: (x[0], len(set(x[1])))).collectAsMap()
edges = edges.map(lambda x: [x[0],x[1]]).collect()
# m/2 as it is undirected graph
m = len(edges)/2

# Calculation of betweeness

max_modularity = 0
max_components = []
betweeness = adjacencyMatrix.flatMap(lambda x: betweeness1(x[0])).map(lambda x: (x[0], x[1])).reduceByKey(
    lambda x, y: x+y).map(lambda x: (tuple(sorted([x[0][0], x[0][1]])), x[1]/2)).sortBy(lambda x: (-x[1], x[0]))

i = 0
f = open(sys.argv[3], "w")

for i in betweeness.collect():
    f.write("('"+i[0][0]+"', '"+i[0][1]+"'), "+str(i[1]))
    f.write("\n")
f.close()

i = 0

max_modularity = -float('inf')
max_community = []
first = betweeness.first()
betweeness = betweeness.filter(lambda x: x[1]== first[1])
while(True):
    
    if(betweeness.count() == 0):
        break
    #print(i)
    max_edges = betweeness.collect()
    #print(max_edges)
    i = i + 1
    #print(max_edge)
    # delete the largest edge from both u1 and u2
    for max_edge in max_edges:
        adjacencyMatrixMap[max_edge[0][1]].remove(max_edge[0][0])
        adjacencyMatrixMap[max_edge[0][0]].remove(max_edge[0][1])
    
    betweeness = sc.parallelize(adjacencyMatrixMap.keys()).flatMap(lambda x: betweeness1(
        x)).reduceByKey(lambda x, y: x+y).map(lambda x: (x[0], x[1]/2)).sortBy(lambda x: -x[1])
    if(betweeness.count() == 0):
        break
    first = betweeness.first()
    betweeness = betweeness.filter(lambda x: x[1]== first[1])

    connected = all_communities(adjacencyMatrixMap)
    #print("Duration "+str(time.time()-start))
    #print(len(connected))
    if(len(connected) > 1):
        # Calculate Modularity :
        modularity_rdd = 0
        
        if(sys.argv[1]=='1'):
            modularity_rdd = sc.parallelize(connected).map(
            lambda x: modularityCase1(x)).sum()
        else : 
            modularity_rdd = sc.parallelize(connected).map(
            lambda x: modularityCase2(x)).sum()
        modularity_rdd = modularity_rdd/(2*m)
        if(modularity_rdd >=  max_modularity):
            max_modularity = modularity_rdd
            max_community = connected

           
max_community = sorted(max_community,key=lambda x : [len(x),x])
f = open(sys.argv[4],"w") 
for community in max_community:
    f.write(str(community).strip("[]"))
    f.write("\n")
f.close()

print("Duration "+str(time.time()-start))
