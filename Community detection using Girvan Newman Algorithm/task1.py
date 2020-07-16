from pyspark import SparkContext, SparkConf
import sys
import json
from operator import add
import string
import csv
import os
from itertools import combinations
from functools import reduce
from graphframes import GraphFrame
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11")

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


if __name__ == "__main__":
    inputFile = sys.argv[1]
    outputFile = sys.argv[2]
    filter_threshold = int(sys.argv[3])

    conf = SparkConf().setAppName("Task1").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    sparkSession = SparkSession(sc)


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

    #print(vertex_set)
    vertices = sparkSession.createDataFrame([list(vertex_set)], ["id"])
    edges = sparkSession.createDataFrame([edge_list],['src', 'dst'])
    g = GraphFrame(vertices, edges)
    g.vertices.show()
    g.edges.show()edges = spark.createDataFrame([('1', '2', 'friend'), 
                               ('2', '1', 'friend'),
                              ('3', '1', 'friend'),
                              ('1', '3', 'friend'),
                               ('2', '3', 'follows'),
                               ('3', '4', 'friend'),
                               ('4', '3', 'friend'),
                               ('5', '3', 'friend'),
                               ('3', '5', 'friend'),
                               ('4', '5', 'follows'),
                              ('98', '99', 'friend'),
                              ('99', '98', 'friend')],
                              ['src', 'dst', 'type'])





