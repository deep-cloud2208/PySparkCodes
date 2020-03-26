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
# PairRDD = BasicRDD.map(lambda x: (x.split(',')[1],x))
#
# print('by map transformation...')
# for i in PairRDD.take(5):
#     print(i)

#-----------------------------------------------------------------------------------------------
# Convert a normal RDD to a key value pair RDD - using "keyBy" transformation
#-----------------------------------------------------------------------------------------------
# pairRDD = BasicRDD.keyBy(lambda x: x.split(',')[1])
#
# print('by keyBy transformation...')
# for i in pairRDD.take(5):
#     print(i)

# print(pairRDD.keys().collect())       # Extract keys
# print(pairRDD.values().collect())     # Extract values
# print(pairRDD.lookup('"20140114"'))

#-----------------------------------------------------------------------------------------------
# PairRDD "mapValues" transformation
# mapValues operates on the value only (the second part of the tuple),
# while map operates on the entire record (tuple of key and value).
#-----------------------------------------------------------------------------------------------
# mapvaluesRDD = pairRDD.mapValues(lambda x: (x.split(',')[2], x.split(',')[4]))
# for i in mapvaluesRDD.take(5):
#     print(i)


#-----------------------------------------------------------------------------------------------
# Aggregation transformations - use Car Sales Data
#-----------------------------------------------------------------------------------------------
carRDD = \
sc.textFile("/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/car_sales_information.csv")


#?? QNS 1 >>> which product was sold the most
#---------------------------------------------
carPairRDD = carRDD.keyBy(lambda x: x.split(',')[3])
salesByProduct = carPairRDD.groupByKey()
salesByProductFinal = salesByProduct.map(lambda x: (x[0],len(list(x[1]))))
sortedBySales = salesByProductFinal.sortBy(lambda x: x[1], ascending=False)

# for i in sortedBySales.take(10):
#     print(i)
# print('###############################')

carProductNames = carRDD.map(lambda x: x.split(',')[3])
productCounts = carProductNames.map(lambda x: (x,1))
topProducts = productCounts.reduceByKey(lambda x,y : (x+y))
sortedProducts = topProducts.sortBy(lambda x: x[1], ascending=False) # what if you use sortByKey

for i in sortedProducts.take(10):
    print(i)

#?? QNS 2 >>> Which product was sold the most by Quantity
# -------------------------------------------------------


#?? QNS 3 >>> Who are the product manufacturers
# ---------------------------------------------


#?? QNS 4 >>> Which model was sold in which country the most
# ----------------------------------------------------------


#?? QNS 5 >>> Statewise sale figure in each country
# -------------------------------------------------


#?? QNS 6 >>> Genderwise distribution of Product Manufacturers
#-------------------------------------------------------------



#?? QNS 7 >>> Details of Car make, Product Name and Total Quantity of the oldest car
#---------------------------------------------------------------------------------



#?? QNS 8 >>> Distribution of colors across Product Names
#------------------------------------------------------


#?? QNS 9 >>> Find coutries and their currencies where cars are sold
#-------------------------------------------------------------------



#?? QNS 10 >>> Which credit has been used the most
#-------------------------------------------------



#?? QNS 11 >>> Country, State and Region wise sale figure
#--------------------------------------------------------


#?? QNS 12 >>> Total sales person in the company
#-----------------------------------------------


#?? QNS 13 >>> Given the sales person id, find his/her sale credentials
#-----------------------------------------------------------------------




#-----------------------------------------------------------------------------------------------
# Aggregation transformations - use Plant Data
#-----------------------------------------------------------------------------------------------
plantRDD = \
sc.textFile('file:///Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/plant_data.csv')

#?? QNS 1 >>> which is the most occuring plant family
#----------------------------------------------------


#?? QNS 2 >>> Plant and Country wise distribution
#------------------------------------------------


#?? QNS 3 >>> Find location (state) of the most plant found in a country
#-----------------------------------------------------------------------



#?? QNS 4 >>> Find the occurance of plant name, plant family and scientific name combination
#-------------------------------------------------------------------------------------------




#-----------------------------------------------------------------------------------------------
# Join transformations - use Emp and Dept Data files
#-----------------------------------------------------------------------------------------------
