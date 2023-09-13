# Databricks notebook source
# MAGIC %run /Repos/sarthaks@outlook.com/adventureworks-processing/Common/CommonModule

# COMMAND ----------

# MAGIC %run /Repos/sarthaks@outlook.com/adventureworks-processing/Common/LoggingWithError

# COMMAND ----------

# MAGIC %run /Repos/sarthaks@outlook.com/adventureworks-processing/Common/GlobalVaraibles

# COMMAND ----------

import os
from pyspark.sql.functions import col

@log_function_call_and_errors(log_errors=True, layerDetails=certifiedLayer)
def sourceRawLayerdata(entityName):
    filepath = rawLandingPath+'/'+entityName+subDirectoryPathWithDateTime
    parent_directory = rawLandingPath+'/'+entityName+subDirectoryPathWithDateTime
    subdirectories_df = spark.createDataFrame(os.listdir('/dbfs'+parent_directory), StringType()).toDF("subdirectory")
    subdirectories_df = subdirectories_df.sort(col("subdirectory"), ascending=False)
    latest_subdirectory_name = subdirectories_df.first()["subdirectory"] + '/'
    df = spark.read.parquet(parent_directory+latest_subdirectory_name)
    return df



# COMMAND ----------


@log_function_call_and_errors(log_errors=True, layerDetails=certifiedLayer)
def getKeyAttributeForGivenEntity(entityName):
    if entityName in key_attributes:
        return key_attributes[entityName]
    else:
        None

# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import *
from functools import reduce

# @log_function_call_and_errors(log_errors=True, layerDetails=certifiedLayer)
def mergedataWithTarget(incrementDataFrame, destinationPath, keyColumns):
    
    if not DeltaTable.isDeltaTable(spark, destinationPath):
        incrementDataFrame.write.format("delta").save(destinationPath)
    else:
        deltaTable = DeltaTable.forPath(spark, destinationPath)
        increment = incrementDataFrame

        join_condition = reduce(lambda a, b: a & b, (col("target." + col_name) == col("updates." + col_name) for col_name in keyColumns))

        deltaTable.alias("target").merge(
        increment.alias("updates"),
        join_condition) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
    
    return True


