# wordcount program
# -*- coding: utf-8 -*-

from __future__ import print_function

import sys
from operator import add

from pyspark import SparkContext


def mapper(line):
    year = line[15:19]
    temp = int(line[87:92])
    # print(line[87:92])
    # print("year: %s, temp: %i" % (year, temp))

    # exclude values of 9999 (no data)
    if temp >= 9999:
        temp = 0

    # scale temperature by 10
    return (year, temp / float(10))

if __name__ == "__main__":
    sc = SparkContext(appName="MaxTemperature")
    lines = sc.textFile(
        "/home/NOAA_DATA/20{08,09,10,11,12}", use_unicode=False)
    #lines = sc.textFile("155500-99999-2008.gz")
    temperatures = lines.map(mapper) \
        .reduceByKey(max)
    output = temperatures.collect()
    for (year, temperature) in output:
        print("Year %s: %i° C" % (year, temperature))

    sc.stop()
