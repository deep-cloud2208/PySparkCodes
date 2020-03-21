from pyspark import SparkContext, SparkConf
import json

conf = SparkConf().setAppName('intellipaat_rdd').setMaster('local')
sc = SparkContext(conf=conf)

#-----------------------------------------------------------------------------------------------
# RDD "filter" Transformation
# Reads all records/lines of an RDD and returns that matches the condition
# Similar to SQL WHERE clause
#-----------------------------------------------------------------------------------------------
# def filter_with_zip(line):
#     line = json.loads(line)
#     zipcode = line['address']['zipcode']
#     zipcode = int(zipcode)
#     if (zipcode > 11000):
#         return line
#
# rdd1 = sc.textFile("/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/restaurants.json")
# rdd2 = rdd1.filter(lambda x: filter_with_zip(x))

# Write the above in a single line lambda function

# for i in rdd2.collect(): # "collect" is an RDD "action"
#     var=json.loads(i)
#     print(var['address']['zipcode'])


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
# rdd2 = rdd1.map(lambda x: get_3_fields(x))
#
# for i in rdd2.collect():
#     print(list(i))


#-----------------------------------------------------------------------------------------------
# RDD "flatMap" transformation
# Takes each record from an RDD and returns multiple rows
#-----------------------------------------------------------------------------------------------
# rdd1 = sc.textFile("file:///Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/stream-data.csv")

# def remove_date(record):
#     record = record.split('>')
#     return record[1]

# rdd2 = rdd1.map(lambda x: x.split('>')[1])
# rdd3 = rdd2.flatMap(lambda x: x.split('|'))
#
# for i in rdd3.collect():
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

# union_rdd = rdd1.union(rdd2)
# print(union_rdd.count())

# intersection_rdd = rdd1.intersection(rdd2)

# for i in union_rdd.collect():
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
#     date = record[0]
#     time = record[1]
#     size = record[2]
#
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