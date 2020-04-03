import pandas as pd

#--------------------------------------------------------------------------------------------
# Note:
# Using pandas to convert small files is better as it does not use any JVM in the background
#--------------------------------------------------------------------------------------------

input_file_name = "/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/covid-19_patients_data.csv"
output_file_name = "/Users/soumyadeepdey/HDD_Soumyadeep/TECHNICAL/Training/Intellipaat/PySparkCodes/sampledata/covid-19_patients_data_pandas.parquet"

df = pd.read_csv(input_file_name)
df.to_parquet(output_file_name)