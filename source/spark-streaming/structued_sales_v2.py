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


#print(lines.isStreaming())
lines.printSchema()

salesDF = lines.select(
    # explode turns each item in an array into a separate row
    split(lines.value, ',')[1].alias("date"),split(lines.value, ',')[2].alias("city"),split(lines.value, ',')[5].cast("float").alias("sales_amt"),  split(lines.value, ',')[6].alias("quantity")
    ).alias('word')

salesDF.printSchema()

#salesDF.createOrReplaceTempView("SalesSummary")
salesSummaryDF = salesDF.groupBy("city").sum("sales_amt")
#spark.sql("select city, sum(sales_amt) salesSum from SalesSummary group by city order by salesSum desc")  # returns another streaming DF
#salesDF.isStreaming()


#userSchema = StructType().add("salesDate", "date").add("quantity", "integer")


#wordCounts = words.groupBy('word').count()

# Start running the query that prints the running counts to the console
query = salesSummaryDF\
    .writeStream\
    .outputMode('Complete')\
    .format('console')\
    .start()

    #.outputMode('Append')\

query.awaitTermination()