from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, sum

# conf = SparkConf().setMaster('local[4]').setAppName('Streaming')
# sc = SparkContext(conf=conf)
# ssc = StreamingContext(sc,60)


#-----------------------------------------------------------------------------------------------------------------------
'''
Streaming concepts: Source --> Ingest --> Process --> Sink --> Analytics
Sources: Kafka, AWS Kinesis, Continuous files in a directory, Socket source (testing), Google Pub/Sub, Custom sources
Sinks: Kafka, Files, BigQuery, Google Pub/Sub, Console sink (testing), Memory sink (testing)

Spark Streaming using DStreams (with Socket Streams > use nc -lk 8000)
Available Tranformations: map(), flatMap(), filter(), repartition(), union(), count(), reduce(), countByValue(), 
                          reduceByKey(), join()
Output Operations: pprint(), saveAsTextFiles(), saveAsObjectFiles()
'''
#-----------------------------------------------------------------------------------------------------------------------
# Socket Source - Using DStreams
#-----------------------------------------------------------------------------------------------------------------------
# socketStreaming = sc.textFile('/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/car_sales_data.csv')
# socketStreaming = ssc.socketTextStream("localhost",20000)
# carRdd1 = socketStreaming.map(lambda x: (x.split(",")[3], x.split(",")[5], x.split(",")[8]))
# carRdd2 = carRdd1.map(lambda x: ((x[0],x[2]),x))        # Streaming RDD/DStreams do not support keyBy
# carRdd3 = carRdd2.map(lambda x: (x[0],int(x[1][1])))
# carRdd4 = carRdd3.reduceByKey(lambda x,y: x+y)
# carRdd4.pprint()
#
# ssc.start()
# ssc.awaitTermination()

#-----------------------------------------------------------------------------------------------------------------------
'''
Spark Structured Streaming
--------------------------
Source ->   
- File source 
- Socket source (Testing) 
- Rate source (Testing) 
- Kafka 
- AWS Kinesis, RedShift 
- Google Pub/Sub, BigQuery

Sink/Target ->      
- File sink (Append) 
- Kafka sink (Append, Update, Complete) 
- Console sink (Append, Update, Complete) 
- Memory Sink (Append, Comp.lete)
- Foreach | ForeachBatch sink (Append, Update, Complete)
- BigQuery (GCP)
- RedShift (AWS)        

Output Modes: 
Complete (Overwrite) | Append | Update

All Structured API transformations and operations are supported EXCEPT for the following:
- Chain of aggregrations on a streaming DataFrame
- Limit and Take
- Distinct
- Sorting (** Only supported after an aggregation and only in COMPLETE mode)
- Some types of Joins (refer documentation)

Streaming DataFrames can be registered as temporary view and then SQL can be applied on them.
'''
#-----------------------------------------------------------------------------------------------------------------------
# File Source - schema has to be provided
# Automatic schema inference can be enabled using "spark.sql.streaming.schemaInference" to "true" (only for file based
# sources)
#-----------------------------------------------------------------------------------------------------------------------
# spark = SparkSession.builder.appName("Streaming").getOrCreate()
# carSchema = StructType(
#     [
#         StructField("product_id", IntegerType(), True),
#         StructField("sales_person_id", StringType(), True),
#         StructField("sales_person_name", StringType(), True),
#         StructField("product_name", StringType(), True),
#         StructField("price", StringType(), True),
#         StructField("quantity_sold", IntegerType(), True),
#         StructField("product_make", StringType(), True),
#         StructField("product_color", StringType(), True),
#         StructField("model_year", StringType(), True),
#         StructField("region_sold_in", StringType(), True),
#         StructField("state_sold_in", StringType(), True),
#         StructField("country_sold_in", StringType(), True),
#         StructField("buyer_gender", StringType(), True),
#         StructField("currency", StringType(), True),
#         StructField("credit_card_type", StringType(), True),
#         StructField("Car_VIN", StringType(), True)
#     ]
# )
# # streamProcessing = spark.read.format("csv").schema(carSchema).load("/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/car_sales_data.csv")
# # streamProcessing = spark.readStream.format("socket").schema(carSchema).option("host","localhost").option("port","8000").load()
# streamProcessing = spark.readStream.format("csv")\
#                                     .schema(carSchema)\
#                                     .option("maxFilesPerTrigger",1)\
#                                     .load("/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/streamFiles/")
#
# df1 = streamProcessing.select("product_name","quantity_sold","model_year","country_sold_in")
# df2 = df1.filter(col('model_year').cast(IntegerType()).__ge__(2000) & col('model_year').cast(IntegerType()).__le__(2010))
# df3 = df2.groupBy("product_name","model_year","country_sold_in").agg(sum("quantity_sold").alias("tot_qty_sold"))
#
# # df3.createOrReplaceTempView("streaming_car_table")
# df4 = df3.writeStream.format("console")\
#                      .outputMode("complete")\
#                      .trigger(processingTime='90 seconds')\
#                      .start()
#
# df4.awaitTermination()


#-----------------------------------------------------------------------------------------------------------------------
# Tumbling Window and Sliding Window based on Event Time
#-----------------------------------------------------------------------------------------------------------------------
from pyspark.sql.functions import to_timestamp, to_date, date_format, col, window

# spark = SparkSession.builder.appName("Streaming").config("spark.sql.streaming.schemaInference", "true").getOrCreate()
# spark = SparkSession.builder.appName("Streaming").getOrCreate()
# fileStreaming = spark.read.format('json').option('inferSchema', 'true').load('/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/streaming-stock-data-json/stock-data-1.json')
# fileStreaming = spark.readStream.format("json"). \
#                                  option("inferSchema", "true"). \
#                                  option("maxFilesPerTrigger", 1). \
#                                  load("/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/streaming-stock-data-json/")

# If we do not get event time from data source, we have to add server processing time as event time as shown below:
# from datetime import datetime
# df0 = fileStreaming.withColumn("event_time",datetime.now())

# df1 = fileStreaming.select("ticker","stock_value", "stock_market_cap", "units_sold", "units_bought",to_timestamp(col("event_time"),"HH:mm:ss").alias("event_time"))
# df2 = df1.select('ticker','stock_value','stock_market_cap','units_sold','units_bought', date_format('event_time','hh:mm:ss').alias('event_time'))

#----------------------------------------------------------------------------------------------------------
# Tumbling Window
# window - contains the column based on which you seek aggregation and the trigger duration on event time
#----------------------------------------------------------------------------------------------------------
# df3 = df2.groupBy(window(col('event_time'),"2 minutes"),'ticker').agg(sum('units_bought').alias('units_bought'), sum('units_sold').alias('units_sold'))
# df4 = df3.writeStream.format("console").outputMode("append").start()
# df4.awaitTermination()
# # ------------------------------------------------------------------------------
# # Sliding Window - Trigger window should always be smaller than Sliding window
# #-------------------------------------------------------------------------------
# df3 = df2.withWatermark("event_time","15 minutes").groupBy(window(col('event_time'), "10 minutes", "2 minutes"),'ticker').agg(sum('units_bought').alias('units_bought'), sum('units_sold').alias('units_sold'))
#
# df3 = df2.groupBy('ticker').agg(sum('units_bought').alias('units_bought'), sum('units_sold').alias('units_sold'))
#
# # df4 = df3.writeStream.format("console").outputMode("update").start()
# df4 = df3.writeStream.format("console").trigger(processingTime="1 minute").outputMode("update").start()
# df4.awaitTermination()


#-----------------------------------------------------------------------------------------------------------------------
# Checkpoinitng
# writeStream...option("checkpointLocation", "/path/of/location")...
#-----------------------------------------------------------------------------------------------------------------------
# df4 = df3.writeStream.format("console") \
#                      .option("checkPointLocation", "/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/IntellipaatSpark/checkPointDir")\
#                      .trigger(processingTime="1 minute")\
#                      .outputMode("update").start()
#
# df4.awaitTermination()







