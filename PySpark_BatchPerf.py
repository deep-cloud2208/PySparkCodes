from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

conf = SparkConf()
sc = SparkContext(conf=conf)
# ss = SparkSession.builder.config('spark.sql.shuffle.partitions','4').getOrCreate()
ss = SparkSession.builder.appName("PySpark_BatchPerf").getOrCreate()

'''
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
                            (Default: local[*])

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
                            to exclude while resolving the dependencies provided in --packages
                            to avoid dependency conflicts.

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
#-------------------------------------------------------------------------------------------------------------------------------------------------

car_file = '/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/car_sales_information.json'

# No. of rows read from the file = 14986
carDf = ss.read.format('json').option('inferSchema','true').load(car_file)
# carDf.show()

from pyspark.sql.functions import col, regexp_replace, sum
from datetime import datetime
from pyspark.sql.types import DecimalType

# print("start time: ",datetime.now().time().strftime('%H:%M:%S:'))
start_time = datetime.now()

# No. of rows, after FILTER = 5049
df1 = carDf.filter(col('quantity_sold').__gt__(100000)).repartition(4)

# No. of rows, after SELECT = 5049
# df0 = df1.withColumn('new_price',regexp_replace(df0.price,"\$",""))
df2 = df1.select('Car_VIN','credit_card_type',regexp_replace(col('price'),"\$","").alias('price'),'product_make','product_name','quantity_sold','state_sold_in')

# No. of rows after GROUPBY = 4844
df3 = df2.groupBy('product_make','product_name','credit_card_type','state_sold_in').agg(sum('quantity_sold').alias('tot_quantity_sold'),sum('price').alias('tot_price'))

# No. of rows after GROUPBY = 611
df4 = df3.groupBy('credit_card_type','state_sold_in').agg(sum('tot_quantity_sold').alias('tot_quantity_sold_st'),sum('tot_price').cast(DecimalType(20,2)).alias('tot_price_st'))

# No. of rows after GROUPBY = 16
df5 = df4.groupBy('credit_card_type').agg(sum('tot_quantity_sold_st').alias('tot_quantity_sold_cc'),sum('tot_price_st').alias('tot_price_cc'))

# No. of rows after GROUPBY = 51
df6 = df4.groupBy('state_sold_in').agg(sum('tot_quantity_sold_st').alias('tot_quantity_sold_statewise'),sum('tot_price_st').alias('tot_price_statewise'))

# df5.explain(True)
df5.show()
# df6.show()

end_time = datetime.now()
print("total time taken: ",end_time - start_time)

# Execute this program using spark-submit as below
# spark-submit --master local <full/path/of/PySpark_BatchPerf.py>

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
'''
#-------------------------------------------------------------------------------------------------------------------------------------------------