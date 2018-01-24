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

    batch_interval = 1 # base time unit (in seconds)
    window_length = 15 * batch_interval
    frequency = 6 * batch_interval

    ssc = StreamingContext(sc, batch_interval)
    ssc.checkpoint("checkpoint")

    addFunc = lambda x, y: x + y
    invAddFunc = lambda x, y: x - y

    lines = ssc.socketTextStream('localhost', int('9999'))
    type(lines)
    salesDstream = lines.map(lambda x: x.split(",")).map(lambda y : (y[2],float(y[5])))
    #salesDstream.pprint()


    salesSummy=salesDstream.reduceByKeyAndWindow(lambda x, y: float(x) + float(y), lambda x, y: float(x) - float(y), 10, 2)
    salesSummyFiltered=salesSummy.filter( lambda  amt: (float(amt[1]) > 0.0))
    #salesSummyFiltered=salesSummy.filter(map x: (x[1] > 0))
    salesSummyFiltered.pprint()
    #SalesAgg=salesDstream.reduceByKey(lambda x, y : x + y)

    #window_counts = salesDstream.reduceByKeyAndWindow(addFunc, invAddFunc, window_length, frequency)
    #window_counts.pprint()

    #.reduceByKeyAndWindow(lambda x, y: int(x) + int(y), lambda x, y: int(x) - int(y), 10, 2)


    #SalesAgg.pprint()
    #lines.pprint()
#    counts = lines.flatMap(lambda line: line.split(" "))\
#                  .map(lambda word: (word, 1))\
#                  .reduceByKey(lambda a, b: a+b)
    #lines.pprint()

    ssc.start()
    ssc.awaitTermination()