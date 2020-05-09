from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import os

# spark-submit

conf = SparkConf()
sc = SparkContext(conf=conf)
# ss = SparkSession.builder.config('spark.sql.shuffle.partitions','4').getOrCreate()
ss = SparkSession.builder.getOrCreate()

'''
Submit the application using spark-submit --master local PySpark_BatchPerf.py
Monitoring on Local Mode: http://localhost:4040
'''
#-------------------------------------------------------------------------------------------------------------------------------------------------
'''
SPARK SUBMIT command usage -->

${SPARK_HOME}/bin/spark-submit --help
Usage: spark-submit [options] <app jar | python file | R file> [app arguments]
Usage: spark-submit --kill [submission ID] --master [spark://...]
Usage: spark-submit --status [submission ID] --master [spark://...]
Usage: spark-submit run-example [options] example-class [example args]

--name NAME                 A name of your application.

--master MASTER_URL         local, spark://host:port, mesos://host:port, yarn, or k8s://https://host:port.
                            (Default: local[*]) ("yarn" is for any cluster that is managed by yarn)

--deploy-mode DEPLOY_MODE   Whether to launch the driver program locally ("client")
                            or on one of the worker machines inside the cluster ("cluster").
                            (Default: client)

--conf PROP=VALUE           Arbitrary Spark configuration property.

--properties-file FILE      Path to a file from which to load extra properties.
                            If not specified, this will look for conf/spark-defaults.conf.

--class CLASS_NAME          Your application’s main class (for Java / Scala apps).

--jars JARS                 Comma-separated list of jars to include on the driver and executor classpaths.

--packages                  Comma-separated list of maven coordinates of jars to include on the driver and executor classpaths.
                            Will search the local maven repo, then maven central and any additional remote repositories given by --repositories.
                            The format for the coordinates should be groupId:artifactId:version.

--exclude-packages          Comma-separated list of groupId:artifactId,
                            to exclude while resolving the dependencies provided in --packages to avoid dependency conflicts.

--repositories              Comma-separated list of additional remote repositories to search for the maven coordinates given with --packages.

--py-files PY_FILES         Comma-separated list of .zip, .egg, or .py files to place on the PYTHONPATH for Python apps.

--files FILES               Comma-separated list of files to be placed in the working directory of each executor.
                            File paths of these files in executors can be accessed via SparkFiles.get(fileName).

--driver-memory MEM         Memory for driver (e.g. 1000M, 2G) (Default: 1024M).
--driver-java-options       Extra Java options to pass to the driver.
--driver-library-path       Extra library path entries to pass to the driver.
--driver-class-path         Extra class path entries to pass to the driver.
                            Note that jars added with --jars are automatically included in the classpath.

--executor-memory MEM       Memory per executor (e.g. 1000M, 2G).
                            (Default: 1G)

--proxy-user NAME           User to impersonate when submitting the application.
                            This argument does not work with --principal / --keytab.

'''
#------------------------------------------------------------------------------------------------------------------------------------------------
# Following setup has to be done to access file in S3
#-------------------------------------------------------------------------------------------------------------------------------------------------
# access_id = os.environ['AWS_ACCESS_KEY_ID']
# access_key = os.environ['AWS_SECRET_ACCESS_KEY']
# hadoop_conf = ss.sparkContext._jsc.hadoopConfiguration()
# hadoop_conf.set("fs.s3a.access.key", access_id)
# hadoop_conf.set("fs.s3a.secret.key", access_key)
# car_file = 's3a://deep-spark-bucket-training/sampledata/car_sales_data.json'

# # No. of rows read from the file = 14986
car_file = '/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/car_sales_data.json'
carDf = ss.read.format('json').option('inferSchema','true').load(car_file)

#----------------------
# Demo 1 - Spark UI
#----------------------
# from pyspark.sql.functions import sum
# df1 = carDf.select('product_name','quantity_sold','model_year').repartition(4)
# from pyspark.sql.functions import col
# df2 = df1.filter(col('model_year').__gt__(2000))
# df3 = df2.groupBy('product_name','model_year').agg(sum('quantity_sold').alias('tot_quantity_sold'))
# df3.show()

#-----------------------------
# Demo 2 - Spark UI on EMR
# #-----------------------------
# if __name__ == "__main__":
#     ss = SparkSession.builder.getOrCreate()
#     car_file = 's3a://deep-spark-bucket-training/sampledata/car_sales_data.json'
#     carDf = ss.read.format('json').option('inferSchema', 'true').load(car_file)
#     from pyspark.sql.functions import sum
#     df1 = carDf.select('product_name','quantity_sold','model_year')
#     df2 = df1.groupBy('product_name','model_year').agg(sum('quantity_sold').alias('tot_quantity_sold'))
#     df2.show()


#----------------------
# Demo 3 - Spark UI
#----------------------
# emp_file = '/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/emp_data_ORIG.csv'
# dept_file = '/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/dept_data.csv'
#
# empDf = ss.read.format('csv').option('header','true').load(emp_file)
# deptDf = ss.read.format('csv').option('header','true').load(dept_file)
# joinDf = empDf.join(deptDf, ['dept_id'], 'inner')
# # joinDf.explain()
# joinDf.show()


#-----------------------------------------------------------------------------------------------------------------------
# Demo 4 - Cache and Persist
# cache() always stores in MEMORY
# persist(storageLevel=<StorageLevel>) can be used to store both in memory and disk
# Storage Level = MEMORY_ONLY | DISK_ONLY | MEMORY_AND_DISK | MEMORY_ONLY_2 | MEMORY_AND_DISK_2
#                 MEMORY_ONLY_SER (Java & Scala) | MEMORY_AND_DISK_SER (Java & Scala)
#-----------------------------------------------------------------------------------------------------------------------
# from pyspark.sql.functions import col, regexp_replace, sum
# from datetime import datetime
# from pyspark.sql.types import DecimalType
#
#
# start_time = datetime.now()
#
# df1 = carDf.filter(col('quantity_sold').__gt__(100000)).repartition(4)
# df2 = df1.select('Car_VIN','credit_card_type',regexp_replace(col('price'),"\$","").alias('price'),'product_make','product_name','quantity_sold','state_sold_in')
# df3 = df2.groupBy('product_make','product_name','credit_card_type','state_sold_in').agg(sum('quantity_sold').alias('tot_quantity_sold'),sum('price').alias('tot_price'))
# # df4 = df3.groupBy('credit_card_type','state_sold_in').agg(sum('tot_quantity_sold').alias('tot_quantity_sold_st'),sum('tot_price').cast(DecimalType(20,2)).alias('tot_price_st'))
# df4 = df3.groupBy('credit_card_type','state_sold_in').agg(sum('tot_quantity_sold').alias('tot_quantity_sold_st'),sum('tot_price').cast(DecimalType(20,2)).alias('tot_price_st')).cache()
# df5 = df4.groupBy('credit_card_type').agg(sum('tot_quantity_sold_st').alias('tot_quantity_sold_cc'))
# df6 = df4.groupBy('credit_card_type').agg(sum('tot_price_st').alias('tot_price'))
# df7 = df6.join(df5, ['credit_card_type'], 'inner')
# df7.show()
# # df7.explain()
# #
# # # df5.explain(True)
# #
# end_time = datetime.now()
# print("total time taken: ",end_time - start_time)

#-------------------------------------------------------------------------------------------------------------------------------------------------
'''
IMPORTANT NOTE: SPARK HISTORY SERVER
Spark Web UI on LOCAL MODE (http://localhost:4040) works for spark-shell, pyspark and spark-submit. But when you use "spark-submit" it will
be there on the screen as long as the job is running. The moment job is complete it will go away, but it will be put in something called Spark
"History Server". History server keeps log of all the completed jobs from "spark-submit" sessions. Following is the way to enable "History Server".
Step-1 > Enable the following properties in configuration file:
        - spark.eventLog.enabled=true
        - spark.eventLog.dir=<some-directory>
        - spark.history.fs.logDirectory=<some-directory>
Step-2 > Go to $SPARK_HOME/sbin and execute "start-history-server.sh"
Step-3 > Access history server at http://localhost:18080

History server stdout|stderr logs are stored at - 
$SPARK_HOME/logs/spark-<user-name>-org.apache.spark.deploy.history.HistoryServer-1-<host-name>.out.*

'''
#-------------------------------------------------------------------------------------------------------------------------------------------------
'''
AWS EMR Related Notes:
1. To use Spark Web UI 
   - Setup SSH Tunnel to Master Node. Use the following command 
     ssh -i <key-pair> -N -L 8000:<ec2-dns>:8088 hadoop@<ec2-dns>
   - From browser http://localhost:8000
   
'''