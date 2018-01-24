from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

def process(time, rdd):
    print("========= %s =========" % str(time))

    try:
        # Get the singleton instance of SparkSession
        spark = SparkSession.builder.master("local[*]").getOrCreate()

        # Convert RDD[String] to RDD[Row] to DataFrame
        rowRdd = rdd.map(lambda w: Row(city=w[0],price=w[1]))
        SalesDF = spark.createDataFrame(rowRdd)

        # Creates a temporary view using the DataFrame.
        SalesDF.createOrReplaceTempView("sales_sumy")
        spark.sql("select city, price from sales_sumy").show()

    except:
        pass

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: network_wordcount.py <hostname> <port>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="PythonStreamingNetworkWordCount")
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.WARN)

    batch_interval = 1 # base time unit (in seconds)
    window_length = 15 * batch_interval
    frequency = 6 * batch_interval

    ssc = StreamingContext(sc, batch_interval)
    #spark = SparkSession.builder.master("local[*]").getOrCreate()
	#spark = SparkSession.builder.appName("sparkStraming").getOrCreate()
    ssc.checkpoint("checkpoint")

    #invAddFunc = lambda x, y: x - y    addFunc = lambda x, y: x + y


    lines = ssc.socketTextStream('localhost', int('9999'))
    type(lines)
    #lines.pprint()
    salesDstream = lines.map(lambda x: x.split(",")).map(lambda y : (str(y[2]),float(y[5])))
    salesDstream.foreachRDD(process)
    #words.foreachRDD(process)
    #salesDstream.pprint()
    #rowRdd = salesDstream.map(lambda w: Row(city=w[0],price=w[1]))
    #rowRdd.pprint()

    #sales_summary = spark.sql("select city, price from sales_sumy").show()
    #spark.sql("select city, price from sales_sumy").show()

    ssc.start()
    ssc.awaitTermination()