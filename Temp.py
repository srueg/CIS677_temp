# wordcount program

from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext


def mapper(line):
    year = line[15:19]
    temp = line[87:92]
    # print(line)
    #print("year: %s, temp: %f" % (year, temp))
    return (year, temp)


def reducer(x, y):
    return min(x, y)

if __name__ == "__main__":
    sc = SparkContext(appName="MaxTemperature")
    lines = sc.textFile("/home/NOAA_DATA/20{08}")
    temperatures = lines.map(mapper) \
        .reduceByKey(reducer)
    output = temperatures.collect()
    for (year, temperature) in output:
        print("%s: %i" % (year, temperature))

    sc.stop()
