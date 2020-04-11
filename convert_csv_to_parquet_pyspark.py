from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DataType, IntegerType, DecimalType, StringType, DateType

conf = SparkConf().setAppName('ConvertCsvToParquet').setMaster('local')
sc = SparkContext(conf=conf)
spark = SparkSession.builder.master('local').getOrCreate()

input_file_name = "/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/covid-19_patients_data_ORIG.csv"
output_file_name = "/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/covid-19_patients_data.parquet"

# myschema = StructType(
#     [
#         StructField("patient_number", IntegerType(), True),
#         StructField("patient_id", StringType(), True),
#         StructField("state_patient_number", StringType(), True),
#         StructField("date_announced", DateType(), True),
#         StructField("age_bracket", IntegerType(), False),
#         StructField("gender", StringType(), False),
#         StructField("detected_city", StringType(), False),
#         StructField("detected_district", StringType(), False),
#         StructField("detected_state", StringType(), False),
#         StructField("current_status", StringType(), False),
#         StructField("notes", StringType(), False),
#         StructField("suspected_contacted_patient", StringType(), False),
#         StructField("nationality", StringType(), False),
#         StructField("status_change_date", DateType(), False),
#         StructField("source_1", StringType(), False),
#         StructField("source_2", StringType(), False),
#         StructField("source_3", StringType(), False),
#         StructField("backup_notes", StringType(), False),
#     ]
# )

InputDf = spark.read.format("csv").option('header','true').load(input_file_name)
InputDf.write.format("parquet").save(output_file_name)

