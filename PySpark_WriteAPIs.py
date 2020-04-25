from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = SparkConf().setAppName('WriteAPIs').setMaster('local')
sc = SparkContext(conf=conf)
ss = SparkSession.builder.appName('WriteAPIs').master('local').getOrCreate()

#-----------------------------------------------------------------------------------------------
# Writing to files - RDD
#-----------------------------------------------------------------------------------------------
# car_file = '/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/car_sales_data.csv'
# output_file = '/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/IntellipaatSpark/OutputFile/car_sales_data_out'

# num_partitions = 16
# rdd = sc.textFile(car_file)
# rdd = sc.textFile(car_file, num_partitions)
# print('Total no of partitions: ',rdd.getNumPartitions())

# rdd1 = rdd.map(lambda x: (x.split(',')[3],x.split(',')[5],x.split(',')[11]))
# rdd1.saveAsTextFile(output_file)
# rdd1.coalesce(1).saveAsTextFile(output_file)
# rdd1.repartition(1).saveAsTextFile(output_file)

# rdd2 = sc.textFile("/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/IntellipaatSpark/OutputFile/car_sales_data_out/part-*")
# print(rdd2.getNumPartitions())

# print(rdd1.count())
# print(rdd2.count())
#
# for i in rdd1.take(5):
#     print(i)


#-----------------------------------------------------------------------------------------------
# Writing to files - DataFrame
# - repartition, repartitionByRange
# - mode: 'append', 'overwrite', 'ignore', 'error', 'errorifexists' (default behavior)
#-----------------------------------------------------------------------------------------------
from pyspark.sql.functions import col
car_file = '/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/car_sales_information.json'
out_file = '/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/IntellipaatSpark/OutputFile/car_sales_information_out'

# df1 = ss.read.format('json').option('inferSchema','true').load(car_file)

# df2 = df1.select('product_name','country_sold_in','quantity_sold','region_sold_in').filter(col('quantity_sold') > 100000).repartition('country_sold_in')
# df2 = df1.select('product_name','country_sold_in','quantity_sold','region_sold_in').filter(col('quantity_sold') > 100000)

# df3 = df2.repartition(3)
# print("No. of partitions :", df3.rdd.getNumPartitions())
# df2.select('country_sold_in').distinct().show()
# print(df2.select(col('country_sold_in').__eq__("India")).count())
# print(df2.select(col('country_sold_in').__eq__("United States")).count())

# df3.write.format('parquet').mode('overwrite').save(out_file)
# df2.write.format('parquet').mode('overwrite').partitionBy("country_sold_in").save(out_file)
# df2.write.format('parquet').mode('overwrite').partitionBy("country_sold_in","region_sold_in").save(out_file)
# df2.write.format('parquet').mode('overwrite').partitionBy("country_sold_in").save(out_file)
# df2.write.format('orc').mode('overwrite').partitionBy("country_sold_in").save(out_file)

# df3 = ss.read.format('json').option('inferSchema','true').load('/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/IntellipaatSpark/OutputFile/car_sales_information_out/part-*')
# df3 = ss.read.format('parquet').option('inferSchema','true').load("/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/IntellipaatSpark/OutputFile/car_sales_information_out/country_sold_in=*")
# df3 = ss.read.format('parquet').option('inferSchema','true').load("/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/IntellipaatSpark/OutputFile/car_sales_information_out/country_sold_in=India/region_sold_in=*/part-*")
# print(df3.count())

