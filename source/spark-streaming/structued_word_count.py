from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

	
    #sc = SparkContext(appName="PythonStreamingNetworkWordCount")
    #log4j = sc._jvm.org.apache.log4j
    #log4j.LogManager.getRootLogger().setLevel(log4j.Level.WARN)

    #spark = SparkSession.builder.master("local[*]").appName("structuredStream").getOrCreate()
spark = SparkSession\
    .builder\
    .appName("StructuredNetworkWordCount")\
    .getOrCreate()	
	#spark = SparkSession.builder.appName("sparkStraming").getOrCreate()
    #ssc.checkpoint("checkpoint")
spark.sparkContext.setLogLevel("WARN")

lines = spark\
        .readStream\
        .format('socket')\
        .option('host', 'localhost')\
        .option('port', 9999)\
        .load()

words = lines.select(
    # explode turns each item in an array into a separate row
    explode(
        split(lines.value, ',')
    ).alias('word')
)

wordCounts = words.groupBy('word').count()

# Start running the query that prints the running counts to the console
query = wordCounts\
    .writeStream\
    .outputMode('complete')\
    .format('console')\
    .start()

query.awaitTermination()