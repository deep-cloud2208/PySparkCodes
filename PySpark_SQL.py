from pyspark.sql import SparkSession

ss1 = SparkSession.builder.appName('SparkSQL1').enableHiveSupport().master('local').getOrCreate()
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
# ss = SparkSession.builder.appName('SparkSQL').config('spark.sql.warehouse.dir','/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/IntellipaatSpark/spark-sql-data-dir').master('local').getOrCreate()

# ss1.sql("create database hivedb")
# ss1.sql("drop database hivedb")
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
car_file='/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/car_sales_information.json'
carDf = ss1.read.format('json').option('inferSchema','true').load(car_file)
# carTable = carDf.createOrReplaceTempView("car_table")
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
# empDf = ss1.sql("SELECT * from hivedb.emp")
from pyspark.sql.functions import col
# empDf.filter(col('first_name').startswith('B')).show()
# empDf.filter(col('dept_id') == 1).show()


