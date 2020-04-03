from pyspark.sql import SparkSession

ss = SparkSession.builder.appName('Intellipaat-Dataframes').master('local').getOrCreate()

# input_file_csv = '/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/emp_data.csv'
# df1 = ss.read.format('csv').option('header','true').option('inferSchema','true').load(input_file_csv)

# input_file_parquet = '/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/covid-19_patients_data.parquet/part-00000-a1ec0671-08c6-4f32-ba02-f76efb498434-c000.snappy.parquet'
# df1 = ss.read.format('parquet').load(input_file_parquet)

# df1.printSchema()

#-----------------------------------------------------------------------------------------------
# Convert RDD to Dataframe
#-----------------------------------------------------------------------------------------------