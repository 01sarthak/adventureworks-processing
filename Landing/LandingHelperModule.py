# Databricks notebook source
# MAGIC %run /Repos/sarthaks@outlook.com/adventureworks-processing/Common/GlobalVaraibles

# COMMAND ----------

# MAGIC %run /Repos/sarthaks@outlook.com/adventureworks-processing/Common/LoggingWithError

# COMMAND ----------

import os

@log_function_call_and_errors(log_errors=True, layerDetails=landingLayer)
def copyFilesFromSource(entityName):

    finaldir = sourceLandingPath+'/'+entityName+subDirectoryPathWithDateTime

    if not os.path.exists('/dbfs'+finaldir):
        dbutils.fs.mkdirs(finaldir)
    else:
        dbutils.fs.rm(finaldir)

    sourcepath = sourceSystemPath+entityName+'/'

    # List files in the source directory
    files = dbutils.fs.ls(sourcepath)

    # Copy each file from the source directory to the destination directory
    for file in files:
        source_file = file.path
        destination_file = os.path.join(finaldir, os.path.basename(source_file))
        dbutils.fs.cp(source_file, destination_file)
    
    return (True,finaldir)

