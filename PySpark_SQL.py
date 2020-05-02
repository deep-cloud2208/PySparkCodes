from pyspark.sql import SparkSession

ss = SparkSession.builder.appName('SparkSQL1').enableHiveSupport().master('local').getOrCreate()
# ss1 = SparkSession.builder.appName('SparkSQL1').master('local').getOrCreate()
# print(ss1.sparkContext.getConf().getAll())

# -------------------------------------------------------------------------------------------------------------------------------------------------
'''
Set up some configuration properties in '$SPARK_HOME/conf'
1. $SPARK_HOME/conf/log4j.properties - log4j.rootCategory to remove INFO messages (from INFO to ERROR)
2. $SPARK_HOME/conf/spark-defaults.conf - set spark.sql.warehouse.dir   
3. $SPARK_HOME/conf/spark-defaults.conf - Set a common location for Derby metastore_db (using the following option)
   spark.driver.extraJavaOptions -Dderby.system.home=/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/IntellipaatSpark/SparkSqlDataDir/derby

   What happens if common Derby location is not set??
   - With enableHiveSupport() option metastore_db is created in the local directory where PySpark is running from
   - Without enableHiveSupport() option the metastore is stored in memory
   - Spark SQL creates a separate metastore_db in the location where spark-sql is run from 
'''
# -------------------------------------------------------------------------------------------------------------------------------------------------
## Note: Change default SQL Warehouse Directory for the application
#--------------------------------------------------------------------------------------------------------------------------------------------------
# ss = SparkSession.builder.appName('SparkSQL').config('spark.sql.warehouse.dir','/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/IntellipaatSpark/SparkSqlDataDir').master('local').getOrCreate()
ss = SparkSession.builder.appName('SparkSQL').master('local').getOrCreate()
ss.sparkContext.setLogLevel("ERROR")

# ss.sql("show databases").show()
# ss.sql("create database hivedb")
# ss.sql("drop database hivedb")
# carDf.write.saveAsTable("hivedb.car_table")
# ss1.sql("select * from hivedb.car_table").show()

# for i in ss1.catalog.listDatabases():
#     print(i)

#-----------------------------------------------------------------------------------------------
## Note: Convert a DataFrame to a SQL Table >>>
##     - createGlobalTempView: Lifetime is tied to the Spark Session that creates the view.
##                             The view is created in "global_temp" database.
##     - createOrReplaceTempView: Lifetime is tied to Spark Application that creates the view.
##     - createTempView: Same as "createOrReplaceTempView".
#-----------------------------------------------------------------------------------------------
# car_file='/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/car_sales_information.json'
# carDf = ss.read.format('json').option('inferSchema','true').load(car_file)
# carDf.createOrReplaceTempView("car_table")

from pyspark.sql.functions import col
# carSql = ss.sql("select product_name, quantity_sold from car_table where quantity_sold > 100000 order by quantity_sold desc")
# ss.sql("select product_name, quantity_sold from car_table where quantity_sold > 100000 order by quantity_sold desc").show()
# carSql.write.mode('append').saveAsTable("intellipaat.car_table")
# carDf.write.mode('append').saveAsTable("intellipaat.car_table")

# carDf.select('product_name', 'quantity_sold').filter(col('quantity_sold') > 100000).orderBy(col('quantity_sold').desc()).show()

# carTable = carDf.createGlobalTempView("car_table")

# carDf.write.saveAsTable('hivedb.car_table')

# ss1.sql("""
# select product_name, quantity_sold from car_table
# """).show()
# ss1.stop()

# ss2 = SparkSession.builder.appName('SparkSQL2').master('local').getOrCreate()
# ss2.sql("""
# select product_name, quantity_sold from car_table
# """).show()

# Access Global Temp View
# ss1.sql("""
# select product_name, quantity_sold from global_temp.car_table
# """).show()

# ss2.sql("""
# select product_name, quantity_sold from global_temp.car_table
# """).show()

# Drop Global Temporary View
# ss1.catalog.dropGlobalTempView("car_table")


#-----------------------------------------------------------------------------------------------
## Note: Read an SQL table and convert it to a DataFrame >>>
##       Create the DataFrame from selecting rows using SELECT statement
#-----------------------------------------------------------------------------------------------
# empDf = ss.sql("SELECT * from intellipaat.car_table")
# from pyspark.sql.functions import col
# empDf.filter(col('first_name').startswith('B')).show()
# empDf.filter(col('dept_id') == 1).show()


#-----------------------------------------------------------------------------------------------
## Execute SQL directly from files
#-----------------------------------------------------------------------------------------------
# df1 = ss.read.format('parquet').load("/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/IntellipaatSpark/OutputFile/car_sales_information_out/country_sold_in=*/region_sold_in=*/part-*")
# df1.printSchema()
# df = ss.sql("select product_name, quantity_sold from parquet.`/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/IntellipaatSpark/OutputFile/car_sales_information_out/country_sold_in=*/region_sold_in=*/part-*`")
# df.show()

#-----------------------------------------------------------------------------------------------
## "bucketBy" operation while saving as table"
## df.format().bucketBy(<no. of buckets>,<list of cols>).saveAsTable(<table name>)
#-----------------------------------------------------------------------------------------------

