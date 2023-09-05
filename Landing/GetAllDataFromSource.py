# Databricks notebook source
# MAGIC %run /Repos/sarthaks@outlook.com/adventureworks-processing/Common/CommonModule
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run /Repos/sarthaks@outlook.com/adventureworks-processing/Landing/LandingHelperModule

# COMMAND ----------

# MAGIC
# MAGIC %run /Repos/sarthaks@outlook.com/adventureworks-processing/Common/GlobalVaraibles

# COMMAND ----------

entityName = 'Address'
result, path = copyFilesFromSource(entityName)
if result:
    print('total files copied from source to landing ' + str(len(dbutils.fs.ls(path))))
