# Databricks notebook source
# MAGIC %run /Repos/sarthaks@outlook.com/adventureworks-processing/Common/GlobalVaraibles

# COMMAND ----------

# MAGIC %run /Repos/sarthaks@outlook.com/adventureworks-processing/Common/LoggingWithError

# COMMAND ----------

# @log_function_call_and_errors(log_errors=True, layerDetails=rawLayer)
def extract_columns(df, schema):
    # Parse the JSON data and extract key attributes into separate columns
    df = df.withColumn("parsed_data", from_json(col("FILEDATA"), schema))

    # Get the fields dynamically from the schema
    fields = [field.split(':')[0] for field in schema.split('struct<')[1].split(', ')]

    # Loop through the attributes dynamically and extract them
    for field in fields:
        df = df.withColumn(field, col("parsed_data")[0][field])

    # Drop the intermediate parsed_data column
    df = df.drop("parsed_data")

    return df


# COMMAND ----------

def get_entity_schema(entity_name):
    # Check if the entity name exists in the dictionary
    if entity_name in entity_schemas:
        return entity_schemas[entity_name]
    else:
        raise ValueError(f"Schema for entity '{entity_name}' is not defined.")


# COMMAND ----------

# entityName = 'Customer'
# inner_schema = get_entity_schema(entityName)
# sourcefilepath = sourceLandingPath+'/'+entityName+subDirectoryPathWithDateTime
# destinationfilepath = rawLandingPath+'/'+entityName+subDirectoryPathWithDateTime
# destinationfileName = entityName+'_'+onlyTimeinHHMMSS

# df = spark.read.format('parquet').load(destinationfilepath+'Customer_142801')
# display(df)
