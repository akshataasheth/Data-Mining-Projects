from pyspark import SparkContext, SparkConf
import sys
import json
from operator import add
import string
import csv
import os
import time
from itertools import combinations
from functools import reduce
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import copy




def calculateEdgeWeights(adjacencyMap, vertexMap, bfs):
    edgeWeightMap = {}
    for i, level in enumerate(bfs[:-1]):
        for vertex in level:
            credit = 1
            if i != 0:
                childNodes = set(adjacencyMap[vertex]).intersection(set(bfs[i - 1]))
                credit = 1 + sum([edgeWeightMap[(min(vertex, childNode), max(vertex, childNode))] for childNode in childNodes])
            parentNodes = set(adjacencyMap[vertex]).intersection(set(bfs[i + 1]))
            vertexWeight = sum([vertexMap[parentNode] for parentNode in parentNodes])
            for parentNode in parentNodes:
                edgeWeight = (float(vertexMap[parentNode])/float(vertexWeight)) * float(credit)
                #print (edgeWeight)
                edgeWeightMap[(min(vertex, parentNode), max(vertex, parentNode))] = edgeWeight
    return edgeWeightMap


def calculateVertexWeights(adjacencyMap, bfs):
    vertexWeight = {}
    levels = len(bfs)
    if levels >= 2:
        for id in bfs[1]:
            vertexWeight[id] = 1
    for level in range(2, levels):
        currentLevel = bfs[level]
        #print(currentLevelNodes)
        parentNodes = bfs[level - 1]
        for node in currentLevel:
            parentNodesConnected = set(adjacencyMap[node]).intersection(set(parentNodes))
            vertexWeight[node] = sum([vertexWeight[parentNode] for parentNode in parentNodesConnected])
    return vertexWeight

  
def computeModularity(vertices,vertexDegree, modularity, modularityMap,edgeCount, adjacencyMap):
        for pair in combinations(vertices, 2):
            i = min(pair[0],  pair[1])
            j = max(pair[0],  pair[1])
            if j in adjacencyMap[i]:
                 a = 1
            else:
                a = 0
            mod = (a - ((0.5 * vertexDegree[i] * vertexDegree[j]) / edgeCount))
            modularityMap[(i, j)] = mod
            modularity += mod
        return modularityMap,modularity

def generateCommunities(currentM,currentCommunities,modularityMap,edgeCount,connectedCommunities):
 for community in connectedCommunities:
    communityModularity = 0
    for pair in combinations(community, 2):
        i = min(pair[0], pair[1])
        j = max(pair[0], pair[1])
        communityModularity += modularityMap[(i, j)]
    currentCommunities.append(sorted(community))
    currentM += communityModularity
    print(currentM)
 currentM = float(1 / (float(2 * edgeCount))) * currentM
 print(currentM)
 return currentM

def generateEdgeVertexList(uid_pairs,uid_bid_rdd):
    edge_list,vertex_set = list(),set()
    for pair in uid_pairs:
        intersection = (set(uid_bid_rdd[pair[0]])).intersection(set(uid_bid_rdd[pair[1]]))
        if len(intersection)>=filter_threshold:
            edge_list.append(tuple(pair))
            edge_list.append(tuple((pair[1],pair[0])))
            vertex_set.add(pair[0])
            vertex_set.add(pair[1])
    return edge_list,vertex_set


def computeBetweenness(vertices,edges,verticesCount, adjacencyMap):
        allEdgeWeights = dict()
        for node in vertices:
            #bfs_tree = bfs(vertices,edges,node)
            bfs_tree = bfs(node,verticesCount,adjacencyMap)
            #print(bfs_tree)
            vertexWeights = calculateVertexWeights(adjacencyMap, bfs_tree)
            vertexWeights[node] = 1
            currentEdgeWeights = calculateEdgeWeights(adjacencyMap,vertexWeights, list(reversed(bfs_tree)))
            #print(currentEdgeWeights.keys())
            for edge in currentEdgeWeights:
                allEdgeWeights[edge] = allEdgeWeights.get(edge, 0) + currentEdgeWeights[edge]
        #print(allEdgeWeights.items())
        return allEdgeWeights.items()
        #return bfs_tree

'''
def bfs (vertices,edges,s):
    visited = dict()
    [visited.setdefault(edge, False) for edge in edges]
    #print(visited.items())
    #visited = [False] * (len(edges)) 
    # Create a queue for BFS 
    queue = [] 
    tree = dict()
    tree[s] = (0, list())
    queue.append(s)
    while queue: 
        s = queue.pop(0) 
        visited[s] = True        
        for i in list(edges[s]):
            if visited[i] == False: 
                queue.append(i) 
                tree[i] = (tree[s][0] + 1, [s])                
                visited[i] = True
            elif tree[s][0]+1 == tree[i][0]:
                tree[i][1].append(s)
    return {k: v for k, v in sorted(tree.items(), key=lambda kv: -kv[1][0])}

'''


def getConnectedGroups(vertices, adjacencyMap):
    visited = set()
    res = []
    for vertex in vertices:
        if vertex not in visited:
            connectedGroup = []
            nodes = {vertex}
            while nodes:
                node = nodes.pop()
                visited.add(node)
                if node in adjacencyMap:
                    unvisited = set(adjacencyMap[node]) - visited
                    if len(unvisited) > 0:
                        nodes = nodes | unvisited
                connectedGroup.append(node)
            res.append(connectedGroup)
    return res


def bfs(vertex, verticesCount, adjacencyMap):
    visited = dict()
    [visited.setdefault(i, False) for i in (adjacencyMap.keys())]
    nextLevel =[]
    bfs = []
    currenLeveltVertices = [] 
    queue = [] 
    queue.append(vertex)
    while queue: 
        s = queue.pop(0) 
        visited[s] = True        
        for i in adjacencyMap[s]:
            if visited[i] == False: 
                visited[i] = True
                nextLevel.append(i)
        currenLeveltVertices.append(s)
        if not queue:
            queue = nextLevel
            nextLevel = []
            #print(currenLeveltVertices)
            bfs.append(currenLeveltVertices)
            currenLeveltVertices = []
    #print(bfs)
    return bfs

def cmp(key1, key2):
    return (key1, key2) if key1 < key2 else (key2, key1)

def count_edges(edges):
        visited = set()
        count = 0
        for start_node, end_nodes in edges.items():
            for end_node in end_nodes:
                key = (start_node, end_node) if start_node < end_node else (end_node, start_node)
                if key not in visited:
                    visited.add(key)
                    count += 1
        return count

if __name__ == "__main__":
    start = time.time()
    inputFile = "ub_sample_data.csv"
    #outputFile = sys.argv[2]
    filter_threshold = 7

    conf = SparkConf().setAppName("Task1").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    sparkSession = SparkSession(sc)
    sc.setLogLevel('ERROR')

    betweennessFilePtr = open("betweness.txt", "w")
    communityFilePtr = open("community.txt", "w")


    dataset = sc.textFile(inputFile).mapPartitions(lambda x : csv.reader(x))
    header = dataset.first()
    dataset = dataset.filter(lambda x: x != header)
    #print(dataset.take(2))
    uid_bid_rdd = dataset.map(lambda x: (x[0],x[1])).groupByKey().mapValues(lambda x: sorted(list(x))).collectAsMap()
    #print(uid_bid_rdd.take(2))

    #generate pairs of uid
    uid_pairs = list(combinations(uid_bid_rdd.keys(),2))
    #print(uid_pairs)
    edge_list,vertex_set = generateEdgeVertexList(uid_pairs,uid_bid_rdd)
    
    verticesList = sc.parallelize(sorted(list(vertex_set)))
    vertices = verticesList.collect()
    edgesList = sc.parallelize(edge_list).groupByKey().mapValues(lambda uidxs: sorted(list(set(uidxs))))
    edges = edgesList.collectAsMap()


    adjacencyMap = {}
    for v in edgesList.collect():
        #print(v)
        adjacencyMap[v[0]] = v[1]

    degreeMap = {}
    for node in vertices:
        degreeMap[node] = len(adjacencyMap[node])

    adjacencyMapDeepCopy = copy.deepcopy(adjacencyMap)
    verticesCount = len(adjacencyMap)


    #betweenness_result = computeBetweenness(vertices,edges,adjacencyMap)
    edgeBetweeness = verticesList.mapPartitions(lambda vertices: computeBetweenness(vertices, edges, verticesCount, adjacencyMap)).reduceByKey(add).map(lambda x: (x[0], x[1] / 2)).sortBy(lambda x: -x[1])
    betweennessT = edgeBetweeness.map(lambda x: (sorted([x[0][0], x[0][1]]), x[1])).sortByKey().sortBy(lambda x: -x[1]).collect()
    betweennessLen = len(betweennessT)

    if betweennessLen > 0:
        for i, e in enumerate(betweennessT):
            betweennessFilePtr.write("('" + str(e[0][0]) + "', '" + str(e[0][1]) + "'), " + str(e[1]))
            if i != betweennessLen - 1:
                betweennessFilePtr.write("\n")
    betweennessFilePtr.close()


    #print(betweennessEdges.take(3))
    modularity = 0
    edgeCount = count_edges(edges)

    modularityMap = {}
    modularityMap, modularity = computeModularity(vertices,degreeMap, modularity, modularityMap,edgeCount,adjacencyMap)

    maxModularity = float(1 / (float(2 * edgeCount))) * modularity
    print(edgeCount)
    print(modularity)
    print(maxModularity)

    communities = getConnectedGroups(vertices, adjacencyMap)

    for index in range(1, edgeCount + 1):
        betweennessT = edgeBetweeness.take(1)[0]
        #print(betweennessT)
        i = betweennessT[0][0]
        j = betweennessT[0][1]
        #print(i)
        #print(j)
        try:
          adjacencyMap[i].remove(j)
          adjacencyMap[j].remove(i)
        except ValueError as e:
          pass

        connectedCommunities = getConnectedGroups(vertices, adjacencyMap)

        currentM = 0
        currentCommunities = []
        print("hellll")
        currentM = generateCommunities(currentM,currentCommunities,modularityMap,edgeCount, connectedCommunities)
        print(currentM)

        if currentM > maxModularity:
            print("helllloo")
            maxModularity = currentM
            communities = currentCommunities

        edgeBetweeness = verticesList.mapPartitions(lambda vertices: computeBetweenness(vertices, edges, verticesCount, adjacencyMap)).reduceByKey(add).map(lambda x: (x[0], x[1] / 2)).sortBy(lambda x: -x[1])

    communities = [sorted([str(i) for i in community]) for community in communities]
    communities = sorted(communities, key=lambda x: (len(x), x))
    #print(communities)
    totalcommunities = len(communities)

    for i, community in enumerate(communities):
        communityFilePtr.write("'" + community[0] + "'")
        if len(community) > 1:
            for node in community[1:]:
                communityFilePtr.write(", ")
                communityFilePtr.write("'" + node + "'")
        if index != totalcommunities - 1:
            communityFilePtr.write("\n")
    communityFilePtr.close()

    print("Duration: ", time.time() - start)