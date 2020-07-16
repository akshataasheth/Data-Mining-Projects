import json
import random
import sys
import time
import csv
from pyspark import SparkContext, SparkConf
from binascii import hexlify


hash_function = 7
buckets = 5000
a = [83, 96, 44, 82, 25, 5, 101, 114, 87, 77, 4, 24, 48, 3, 100, 31, 35, 85, 76, 28, 37, 65, 94, 67, 10, 107, 22, 32, 57, 109, 81, 111, 59, 11,
    6, 19]

b= 83

def makePrediction(city, bitMap):
    if city != "" and city is not None:
        i = int(hexlify(city.encode('utf8')), 16)
        hashList = set(generateSignature(i))
        if hashList.issubset(bitMap):
            yield 1
        else:
             yield 0
    else:
        yield 0



def generateSignature(x):
    hash_array = list()
    for i in range (0,hash_function):
        hash_array.append(((a[i] * x + b) % 233333333333) % buckets)
    return hash_array



if __name__ == '__main__':
    start_time = time.time()
    inputFirst = sys.argv[1]
    inputSecond = sys.argv[2]
    output = sys.argv[3]

    conf = SparkConf().setMaster("local[*]").setAppName("task1_assignment5")
    sc = SparkContext(conf=conf)

    dataset = sc.textFile(inputFirst).map(lambda x: json.loads(x)).map(lambda x: x['city']).distinct().filter(lambda city: city != "").map(lambda city: int(hexlify(city.encode('utf8')), 16))

    citiesHash = dataset.flatMap(lambda index: generateSignature(index)).collect()
    bitMap = set(citiesHash)    
    res = sc.textFile(inputSecond).map(lambda x: json.loads(x)).map(lambda x: x['city']).flatMap(lambda city: makePrediction(city, bitMap))
    results = res.collect()

    with open(output, "w+", newline="") as output_file:
        writer = csv.writer(output_file, delimiter=' ')
        writer.writerow(results)

    print("Duration: %d s." % (time.time() - start_time))