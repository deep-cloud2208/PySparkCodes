from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession

# Lets see SparkContext in a little bit more detail :--
# conf = SparkConf().setMaster('local')
# sc = SparkContext(conf=conf)
# sc2 = SparkContext(conf=conf)
# sql = SQLContext(sc)

# # lets see the SparkContext object
# print(">> SparkContext Object 1  : ",sc)
# print(">> SparkContext Object 2  : ",sc2) # can be solved by setting spark.driver.allowMultipleContexts to 'true'
# print(">> SQLContext Object      : ",sql)
# print(">> StreamingContext Object: ",stream)
# print("SparkContext Object: ",sc2)


#-----------------------------------------------------------------------------------------------
'''
What is SparkSession?
SparkSession = SparkContext + SQLContext + HiveContext + StreamingContext
SparkSession can be created from an existing SparkContext using SQLContext
'''
#-----------------------------------------------------------------------------------------------
# ss = sql.sparkSession # From PREVIOUS SPARKCONTEXT
ss = SparkSession.builder.appName('Intellipaat-Dataframes').master('local').getOrCreate()
# ss2 = ss.newSession()
# ss3 = ss.newSession()
#
# print("1st SparkSession: ",ss)
# print("2nd SparkSession: ",ss2)
# print("3rd SparkSession: ",ss3)
# print("SparkContext of ss {} ".format(ss.sparkContext))
# print("SparkContext of ss {} ".format(ss2.sparkContext))
# print("SparkContext of ss {} ".format(ss3.sparkContext))


#-----------------------------------------------------------------------------------------------
'''
Reading/Writing from/to Data Sources 
CSV     > ss.read.format('csv').option().option().schema(<schema-name>).load(<file>)
          ss.write.format('csv').mode(<write-mode-options>).option().option().save(<file>)
           
Parquet > ss.read.format('parquet').option().option().load(<file>) # SCHEMA IS NOT NEEDED
          ss.write.format('parquet').option().mode(<write-mode-options>).save(<file>)
          
JSON    > ss.read.format('json').option().option().schema(<schema-name>).load(<file>)
          ss.write.format('json').option().mode(<write-mode-options>).save(<file>)
          
ORC     > ss.read.format('orc').option().option().load(<file>) # SCHEMA IS NOT NEEDED
          ss.write.format('orc').option().mode(<write-mode-options>).save(<file>)
          
write modes = "append" | "overwrite" | "errorIfExists" | "ignore"

** Data can be read from JDBC sources too.
'''
#-----------------------------------------------------------------------------------------------
# input_file_csv = '/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/emp_data.csv'
# df1 = ss.read.format('csv').option('header','true').option('inferSchema','true').load(input_file_csv)

# input_file_parquet = '/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/covid-19_patients_data.parquet/part-00000-a1ec0671-08c6-4f32-ba02-f76efb498434-c000.snappy.parquet'
# df1 = ss.read.format('parquet').load(input_file_parquet) # No need to provide any schema
# df1.printSchema()

# input_file_json = '/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/car_sales_information.json'
# df1 = ss.read.format('json').option('inferSchema','true').load(input_file_json)

# df1 = sql.read.format('csv').option('inferSchema','true').load(input_file_json)
# df1.show()


#-----------------------------------------------------------------------------------------------
'''
Spark Data Types (has to be imported from pyspark.sql.types)
- ByteType() > 1byte 
- ShortType() > 2bytes
- IntegerType()
- LongType() > 8bytes
- FloatType(), DecimalType()
- StringType()
- BinaryType(), BooleanType()
- TimestampType() > Python type datetime.datetime
- DateType() > Python type datetime.date
- ArrayType(<elementType>) > list, tuple (elementType should be from above list)
- MapType(<keyType>,<valueType>) > Python dictionary
- StructType() > To define the entire schema
- StructField() > To define individual fields  

Structure to define schema:
StructType([ StructField(), StructField(), StructField() ])

To check the schema use printSchema().
'''
#-----------------------------------------------------------------------------------------------
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, ArrayType, FloatType, DateType
#
# input_restaurant_file = '/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/restaurants.json'
# input_emp_file = '/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/emp_data.csv'
#
# emp_schema = StructType(
#     [
#         StructField("dept_id", IntegerType(),False),
#         StructField("first_name", StringType(), False),
#         StructField("last_name", StringType(), False),
#         StructField("email", StringType(), False),
#         StructField("role", StringType(), False)
#     ]
# )
#
# restaurant_schema = StructType(
#     [
#         StructField("address",StructType([StructField("building",StringType(),False),
#                                           StructField("coord",ArrayType(FloatType()),False),
#                                           StructField("street",StringType(),False),
#                                           StructField("zipcode",StringType(),False)]),
#                      False),
#         StructField("borough",StringType(),False),
#         StructField("cuisine",StringType(),False),
#         StructField("grades",ArrayType(StructType([StructField("date",StructType([StructField("$date",IntegerType(),False)]),False),
#                                                    StructField("grade",StringType(),False),
#                                                    StructField("score",IntegerType(),False)]))
#                      ,False),
#         StructField("name",StringType(),False),
#         StructField("restaurant_id",IntegerType(),False)
#     ]
# )

# df1 = ss.read.format('json').schema(restaurant_schema).load(input_restaurant_file)
# df1 = ss.read.format('json').load(input_restaurant_file).schem(schema) # Will give error
# df1.printSchema()

# df2 = ss.read.format('csv').schema(emp_schema).load(input_emp_file)
# df2.printSchema()


#-----------------------------------------------------------------------------------------------
'''
Convert RDD to Dataframe
- createDataFrame(<rdd>,schema=<myschema>)
  1. Create an RDD
  2. Convert it to type Row, Tuple, List etc.
  3. Define schema
  3. Apply createDataFrame method on the RDD created in step 2.
  Note: RDD need to have type Row, Tuple, List, Int, Boolean, Pandas DataFrame. If RDD is 
        created from CSV/JSON files then it has to be transformed to contain new RDD with
        above data types. Then the new RDD can be converted to a DataFrame.
          
- toDF(<list_of_column_names>) 
  Note: RDD has to be passed with type Row, Tuple, List, Int, Boolean, just the way it is
        done with 'createDataFrame'. The column names have to be passed as a LIST in the 
        argument of toDF. If we do not pass List of column names then the column names would 
        be numbered as _1, _2, _3 and so on.
        
- From an existing SparkContext using SQLContext
  df = sql.read.format('')...
'''
#-----------------------------------------------------------------------------------------------
from pyspark.sql.types import StringType, StructField, StructType, IntegerType
# input_file_csv = '/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/emp_data.csv'

# # from pyspark import SparkContext, SparkConf
# # conf = SparkConf().setMaster('local')
# # sc = SparkContext(conf=conf)
# sc = ss.sparkContext
#
# schema = StructType(
#     [
#         StructField("dept_id", IntegerType(), False),
#         StructField("first_name", StringType(), False),
#         StructField("last_name", StringType(), False),
#         StructField("email", StringType(), False),
#         StructField("role", StringType(), False),
#     ]
# )
#
# def convert_to_list(x):
#     x = x.split(',')
#     # return int(x[0]),x[1],x[2],x[3],x[4]
#     return int(x[0]),x[1],x[2],x[3],x[4]
#
# inputRdd1 = sc.textFile(input_file_csv)
# inputRdd2 = inputRdd1.map(lambda x: convert_to_list(x))
# inputDf = ss.createDataFrame(inputRdd2,schema=schema)

# inputDf2 = inputRdd2.toDF(["dept_id","first_name","last_name","email","role"])
# inputDf2.show()


#-----------------------------------------------------------------------------------------------
# col, column function
# Note: Please install pyspark-stubs to use col and column functions. This is needed for PyCharm
#       IDE as these functions are resolved at runtime.
#-----------------------------------------------------------------------------------------------
from pyspark.sql.functions import col, column

# input_jpmc_file = '/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/JPMC_Bank_Database.csv'
# ss = SparkSession.builder.getOrCreate()

# df1 = ss.read.format('csv').option('header','true').load(input_jpmc_file)
# df1.show()
# print(df1.columns)
# df1.printSchema()

# df1.select(col("Branch_Name").startswith("J"))
# df1.select("Branch_Name","2010_Deposits").filter(col("2010_Deposits").cast(IntegerType()) > 1000000).show(5)

# df2 = df1.select("Branch_Name","2010_Deposits")
# df2.show()
# df1.select("Branch_Name","Established_Date").filter(df1["Branch_Name"].startswith("JP")).show()


#-----------------------------------------------------------------------------------------------
'''
DataFrame Transformations
- select
- limit
- filter | where
- orderBy
- sort
- union
- groupBy
- join
- agg
'''
#-----------------------------------------------------------------------------------------------
input_jpmc_file = '/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/JPMC_Bank_Database.csv'
df1 = ss.read.format('csv').option('header','true').load(input_jpmc_file)

# df1.printSchema()
# df1.select('Institution_Name','Branch_Name','Established_Date').show()
# df1.select('Institution_Name','Branch_Name','Established_Date').limit(10).show()
# df2 = df1.select('Institution_Name','Branch_Name','Established_Date')
# df3 = df2.filter(col("Branch_Name").startswith("J"))
# df3 = df2.where(col("Branch_Name").startswith("J"))
# df3.show()
# print(df3.count())

# df2 = df1.select("Branch_Name", "2010_Deposits","Established_Date")
# df3 = df2.orderBy(col("2010_Deposits").cast(IntegerType()).desc())
# df3 = df2.sort(col("2010_Deposits").cast(IntegerType()).desc())
# df3.show()


