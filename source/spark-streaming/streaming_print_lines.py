from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: network_wordcount.py <hostname> <port>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="PythonStreamingNetworkWordCount")
    #log4j = sc._jvm.org.apache.log4j
    #log4j.LogManager.getRootLogger().setLevel(log4j.Level.WARN)
    ssc = StreamingContext(sc, 2)

    lines = ssc.socketTextStream('localhost', int('9999'))
    type(lines)
    salesDstream=lines.map(lambda x: x.split(",")).map(lambda y : (y[2],y[5]))
    SalesAgg=salesDstream.reduceByKey(lambda x, y : x + y)
    SalesAgg.pprint()
    #lines.pprint()
#    counts = lines.flatMap(lambda line: line.split(" "))\
#                  .map(lambda word: (word, 1))\
#                  .reduceByKey(lambda a, b: a+b)
#    counts.pprint()

    ssc.start()
    ssc.awaitTermination()