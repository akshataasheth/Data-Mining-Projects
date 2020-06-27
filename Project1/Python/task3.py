import json
import sys
from operator import add
import hashlib
import binascii
import time
from pyspark import SparkContext,SparkConf

start_time = time.time()
print(start_time)

def count_in_a_partition(iterator):
    yield sum(1 for _ in iterator)

def review_partitioner(iterator):
    #return int(hashlib.md5(iterator.encode('utf-8')).hexdigest(),16)
    return binascii.crc32(iterator[:1].encode('utf-8'))

if __name__ == '__main__':
    reviewFile = sys.argv[1]
    outputFile = sys.argv[2]
    partitionType = sys.argv[3]
    partitionNumber = sys.argv[4]
    n = sys.argv[5]

    results = {}

    conf = SparkConf().setAppName("Task3").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    reviewData = sc.textFile(reviewFile).map(lambda row: json.loads(row))
    #Emit key value pairs or the form: (business_id,1) for each business_id
    reviewRDD = reviewData.map(lambda x: (x['business_id'], 1))

    if partitionType != "default":
        reviewRDD = reviewRDD.partitionBy(int(partitionNumber), review_partitioner)

    results['n_partitions'] = reviewRDD.getNumPartitions()
    results['n_items'] = reviewRDD.mapPartitions(count_in_a_partition).collect()
    print( results['n_items'])


    resultsRDD = reviewRDD.reduceByKey(add)
    results['result'] = resultsRDD.filter(lambda x: x[1] > int(n)).collect()

    with open(outputFile, 'w') as output_file:
        json.dump(results, output_file)
    output_file.close()