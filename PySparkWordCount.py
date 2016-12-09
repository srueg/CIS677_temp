# wordcount program

from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext

def mapper(word):
        return (word, 1)

def reducer(x,y):
        return x+y

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="PySparkWordCount")
    lines = sc.textFile(sys.argv[1], 1)
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(mapper) \
                  .reduceByKey(reducer)
    output = counts.collect()
    for (word, count) in output:
        print("%s: %i" % (word, count))

    sc.stop()

