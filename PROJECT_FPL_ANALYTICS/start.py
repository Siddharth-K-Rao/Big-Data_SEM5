#!/usr/bin/python3

"""
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
sc = SparkContext("local[2]", "NetworkWordCount") #2 represents number of threads
ssc = StreamingContext(sc, 1) #1 represents the batch difference
lines = ssc.socketTextStream("localhost", 9999)
ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate

from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: network_wordcount.py <hostname> <port>", file=sys.stderr)
        sys.exit(-1)
    sc = SparkContext(appName="PythonStreamingNetworkWordCount")
    ssc = StreamingContext(sc, 1)

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    counts = lines.flatMap(lambda line: line.split(" "))\
                  .map(lambda word: (word, 1))\
                  .reduceByKey(lambda a, b: a+b)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
"""
(26010, (0.14774305555555556, 1609))
(7879, (0.020833333333333332, 1609))
(7870, (0.16606040564373897, 1609))
(370224, (-0.4585831160365058, 1609))
(120339, (0.2721288515406163, 1609))
(7945, (0.0017387218045112895, 1609))
(14869, (0.0033232097186700615, 1609))
(25413, (-0.4552734375, 1609))
(7868, (0.0035357954545454096, 1609))
(3560, (0.384698275862069, 1609))
