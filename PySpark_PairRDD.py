from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('IntellipaatPairRDD').setMaster('local')
sc = SparkContext.getOrCreate(conf=conf)
#-----------------------------------------------------------------------------------------------
# Pair RDDs are RDDs with Key-Value pairs. It is needed most of the cases than normal RDDs.
# Whenever you see <some-operation>ByKey, that transformation can only be performed on pair
# RDDs.
#-----------------------------------------------------------------------------------------------
# Convert a normal RDD to a key value pair RDD - using "map" transformation
#-----------------------------------------------------------------------------------------------
BasicRDD = sc.textFile('/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/violations_plus.csv')
PairRDD = BasicRDD.map(lambda x: (x.split(',')[1],x))

# for i in PairRDD.collect():
#     print(i)

#-----------------------------------------------------------------------------------------------
# Convert a normal RDD to a key value pair RDD - using "keyBy" transformation
#-----------------------------------------------------------------------------------------------
pairRDD = BasicRDD.keyBy(lambda x: x.split(',')[1])

# for i in PairRDD.take(5):
#     print(i)

# print(pairRDD.keys().collect())       # Extract keys
# print(pairRDD.values().collect())     # Extract values

#-----------------------------------------------------------------------------------------------
# PairRDD "mapValues" transformation
# mapValues operates on the value only (the second part of the tuple),
# while map operates on the entire record (tuple of key and value).
#-----------------------------------------------------------------------------------------------
mapvaluesRDD = pairRDD.mapValues(lambda x: (x.split(',')[2], x.split(',')[4]))

for i in mapvaluesRDD.take(5):
    print(i)