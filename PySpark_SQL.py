from pyspark.sql import SparkSession

ss = SparkSession.builder.appName('SparkSQL').master('local').getOrCreate()
car_file='/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/car_sales_information.json'
carDf = ss.read.format('json').option('inferSchema','true').load(car_file)
carTable = carDf.createOrReplaceTempView("car_table")

ss.sql("""
select product_name, quantity_sold from car_table
""")


