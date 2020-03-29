from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('local').setAppName('Intellipaat')
sc = SparkContext(conf=conf)


#-----------------------------------------------------------------------------------------------
# RDD "filter" Transformation
# Reads all records/lines of an RDD and returns that matches the condition
# Similar to SQL WHERE clause
#-----------------------------------------------------------------------------------------------
# import json
# def filter_with_zip(line):
#     line = json.loads(line)
#     # zipcode = line['address']['zipcode']
#     # zipcode = int(zipcode)
#     # if (zipcode > 11000):
#     cuisine = line["cuisine"]
#     if (cuisine == 'Bakery'):
#         return line
#
# rdd1 = sc.textFile("/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/restaurants.json")
#
# rdd2 = rdd1.filter(lambda x: filter_with_zip(x))

# # Write the above in a single line lambda function

# for i in rdd2.collect(): # "collect" is an RDD "action"
#     print(i)
#     var=json.loads(i)
#     print(var['address']['zipcode'])

# print('Total records in rdd1: ',rdd1.count())
# print('Total records in rdd2: ',rdd2.count())


#-----------------------------------------------------------------------------------------------
# RDD "map" Transformation
# Map reads all records one by one, does processing on those records and returns the exact
# no. of records in the output rdd.
#-----------------------------------------------------------------------------------------------
# rdd1 = sc.textFile("file:///Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/sample-data.csv")
#
# def get_3_fields(record):
#     x,y,z = record.split(',')[0],record.split(',')[3],record.split(',')[5]
#     return x,y,z
#
# rdd2 = rdd1.map(lambda x: (x.split(',')[0], x.split(',')[3], x.split(',')[5]))
#
# for i in rdd2.collect():
#     print(list(i))


#-----------------------------------------------------------------------------------------------
# RDD "flatMap" transformation
# Takes each record from an RDD and returns multiple rows
#-----------------------------------------------------------------------------------------------
# rdd1 = \
# sc.textFile("file:///Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/stream-data.csv")

# def remove_date(record):
#     record = record.split('>')
#     return record[1]

# rdd2 = rdd1.map(lambda x: x.split('>')[1])
# rdd3 = rdd2.flatMap(lambda x: x.split('|'))
# rdd4 = rdd3.map(lambda x: (x.split('-')[0], x.split('-')[1]))
#
# for i in rdd4.collect():
#     print(i)


#-----------------------------------------------------------------------------------------------
# RDD "union/intersection" transformation
# UNION Merges two RDDs - does not matter whether the RDDs have same data or not. But a better use
# case would be to merge two RDDs with same data structure or schema. UNION WILL NOT REMOVE
# DUPLICATE VALUES.
# Intersection fetches common values from 2 RDDs. It will also remove duplicates whilst doing
# the intersection.
#-----------------------------------------------------------------------------------------------

# rdd1 = sc.textFile("/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/stream-data.csv")
# rdd2 = sc.textFile("/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/stream-data2.csv")
# rdd3 = sc.textFile("/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/violations_plus.csv")

# union_rdd = rdd1.union(rdd3)
# print(union_rdd.count())

# intersection_rdd = rdd1.intersection(rdd3)

# for i in union_rdd.collect():
# for i in union_rdd.take(50):
# for i in intersection_rdd.collect():
#     print(i)


#-----------------------------------------------------------------------------------------------
# RDD "sortBy" transformation
# Used to sort the entire RDD based on ONE key. We can specify the key ourselves
#-----------------------------------------------------------------------------------------------
# rdd = sc.textFile("/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/2015-12-12.csv")
#
# def convertToInteger(record):
#     record = record.split(',')
#     size = record[2]
#     try:
#         size = int(size)
#         return record
#     except ValueError:
#         pass
#
# rdd1 = rdd.filter(lambda x: convertToInteger(x))
# rdd2 = rdd1.sortBy(lambda x: x.split(',')[2],ascending=False)
#
# for i in rdd2.take(20):
#     print(i)

#-----------------------------------------------------------------------------------------------
# RDD "groupBy" transformation
# Similar to sortBy. We will have to pass the key based on which we want to group the
# elements of the RDD.
#-----------------------------------------------------------------------------------------------
# rdd = sc.textFile("/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/violations_plus.csv")
#
# rdd1 = rdd.map(lambda x: (x.split(',')[0], x.split(',')[3]))
# rdd2 = rdd1.groupBy(lambda x: x[0])
# rdd3 = rdd2.map(lambda x:(x[0],list(x[1])))
#
# for i in rdd3.take(5):
# # for i in rdd2.collect():
#     print(i)

#-----------------------------------------------------------------------------------------------
# RDD "reduce" transformation
#-----------------------------------------------------------------------------------------------z
# carRDD = sc.textFile("/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/car_sales_information_copy.csv")
#
# from decimal import Decimal, InvalidOperation
# def trim_price(record):
#     price = record.split(',')[4]
#     price = price.strip('$')
#
#     try:
#         try:
#             price = Decimal(price)
#             return price
#         except InvalidOperation:
#             pass
#
#     except ValueError:
#         pass
#
# def filter_none(record):
#     if record is not None:
#         return record
#
# rdd1 = carRDD.map(lambda x: trim_price(x))
# rdd2 = rdd1.filter(lambda x: filter_none(x))
# rdd_total = rdd2.reduce(lambda x,y: x+y)
# rdd_max = rdd2.reduce(lambda x,y: max(x,y))
# rdd_min = rdd2.reduce(lambda x,y: min(x,y))
#
# print("Total Sales: ",rdd_total)
# print("Max Sales: ",rdd_max)
# print("Min Sales: ",rdd_min)


# def total_records_per_partition(record):
#     total_record = 0
#     for x in record:
#         total_record = total_record + 1
#     return total_record
#
# rdd2 = rdd1.mapPartitions(lambda x: total_records_per_partition(x))
#
# print(rdd2.collect())

# for i in rdd2.collect()