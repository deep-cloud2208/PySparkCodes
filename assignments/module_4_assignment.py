from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('local').setAppName('Module4_assignment')
sc = SparkContext(conf=conf)

# -------------------
# 1. Load the dataset
# -------------------
rdd1 = sc.textFile("file:////Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/assignments/Mod4_AB_NYC_2019.csv")

# --------------------------
# 2. Print the first 10 rows
# --------------------------
# print(rdd1.take(10))

# -------------------------------------------------------------------
# 3. Find the total number of private rooms in the ‘room_type’ column
# -------------------------------------------------------------------
# print("Total rooms: ", rdd1.count())
#
# def private_room(record):
#     var = record.split(',')
#     if len(var) > 9: # Becasue one of the record does not have all fields
#         room_type = var[8]
#         if room_type == "Private room":
#             return record
#
# private_room_rdd = rdd1.filter(lambda x: private_room(x))
# print("Total Private Rooms: ",private_room_rdd.count())

# for i in private_room_rdd.collect():
#     print(i)

# -----------------------------------------------------
# 4. Find the max, min, and average of the price column
# -----------------------------------------------------
# from decimal import Decimal
#
# def price(record):
#     var = record.split(',')
#     if len(var) > 10: # Becasue one of the record does not have all fields
#         price = Decimal(var[9])
#         return price

# price_rdd = rdd1.map(lambda x: x.split(',')[9])
# price_rdd = rdd1.map(lambda x: price(x))

# print("Max price: ", price_rdd.max())
# print("Min price: ", price_rdd.min())
# print("Avg price: ", price_rdd.avg())

# print(price_rdd.take(1000))

# ------------------------------------------------------------------------------------
# 5. Find the number of rooms available for booking for less than 200 days a year (use
# the ‘availability_365’ column)
# ------------------------------------------------------------------------------------
# def get_availability(record):
#     record = record.split(',')
#     availability = record[-1]
#
# # Following piece of code is because of the file corruption
#     try:
#         availability = int(availability)
#
#         if availability > 200:
#             return record
#     except ValueError:
#         pass
#
# avail_for_booking_rdd = rdd1.filter(lambda x: get_availability(x))
#
# for i in avail_for_booking_rdd.collect():
#     print(i)

# -----------------------------------------------------------
# 6. Find 10 host places that have the most number of reviews
# -----------------------------------------------------------
# def reviews(record):
#     record = record.split(',')
#     record_len = len(record)
#     if record_len > 11: # Only because the file is corrupted
#         no_of_reviews = record[11]
#         try:            # Only because the file is corrupted
#             no_of_reviews = int(no_of_reviews)
#             return record
#         except ValueError:
#             pass
#
# reviews_rdd = rdd1.filter(lambda x: reviews(x))
# reviews_rdd2 = reviews_rdd.map(lambda x: (x.split(',')[3], x.split(',')[11]))
# reviews_sorted_rdd = reviews_rdd2.sortBy(lambda x: x[1],ascending=False)
#
# for i in reviews_sorted_rdd.take(100):
#     print(i)
