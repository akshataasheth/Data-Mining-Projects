from pyspark import SparkContext, SparkConf
import sys
import json
from operator import add
import string


def clean(word):
    if word not in stop_words:
        return ''.join(c for c in word if c not in ('(', '[', ',', '.', '!', '?', ':', ';', ']', ')'))


if __name__ == "__main__":
    conf = SparkConf().setAppName("Task1").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    reviewDataset = sc.textFile(sys.argv[1]).map(lambda x: json.loads(x))
    outputFile = sys.argv[2]
    stopwords = sys.argv[3]
    y = sys.argv[4]
    m = int(sys.argv[5])
    n = int(sys.argv[6])
    exclude_char_set = set(r"""()[],.!?:;""")
    stop_words = set()
    for line in open(stopwords):
       stop_words.add(line.strip())
    results = {}
    #reviewRDD = sc.textFile("review.json")
    #reviewDataset = reviewRDD.map(json.loads)
    reviewDataset.persist()

    results['A'] = reviewDataset.map(lambda review: review['review_id']).distinct().count()
    results['B'] = reviewDataset.filter(lambda review: review['date'].split("-")[0] == y).count()
    results['C'] = reviewDataset.map(lambda review: review['user_id']).distinct().count()
    results['D'] = reviewDataset.map(lambda review: (review['user_id'], 1)).reduceByKey(add).sortByKey().sortBy(lambda x: x[1], ascending=False).take(m)
    
    #split words
    rdd = reviewDataset.flatMap(lambda review: review['text'].lower().split(' '))

    # Map a tuple and append int 1 for each word
    rdd = rdd.map(lambda x: (clean(x),1)).filter(lambda k: (k[0] is not "") and (k[0] is not None))

    # Perform aggregation (sum) all the int values for each unique key and take n top words
    resultrdd = rdd.reduceByKey(add).takeOrdered(n, key=lambda k: (-k[1], k[0]))
    results['E'] = list(map(lambda k: k[0], resultrdd))
    
    #Write output to output.json
    with open('output.json', 'w') as f:
     json.dump(results, f, sort_keys=True)
    f.close()