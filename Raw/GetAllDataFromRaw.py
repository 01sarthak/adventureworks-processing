# Databricks notebook source
# MAGIC %run /Repos/sarthaks@outlook.com/adventureworks-processing/Raw/RawHelperModule

# COMMAND ----------

# MAGIC %run /Repos/sarthaks@outlook.com/adventureworks-processing/Common/CommonModule

# COMMAND ----------

from pyspark.sql.functions import from_json, col

entityName = 'Address'
inner_schema = get_entity_schema(entityName)
sourcefilepath = sourceLandingPath+'/'+entityName+subDirectoryPathWithDateTime
destinationfilepath = rawLandingPath+'/'+entityName+subDirectoryPathWithDateTime
destinationfileName = entityName+'_'+onlyTimeinHHMMSS

json_schema = StructType([
    StructField("FILEDATA", StringType(), True)
])

df = spark.read.schema(json_schema).json(sourcefilepath)
df = extract_columns(df, inner_schema)
df = df.drop("FileData")
df.write.parquet(destinationfilepath+destinationfileName)

print('File Saved in Destination path. Total records in file: ' + str(df.count()))
