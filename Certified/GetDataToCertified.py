# Databricks notebook source
# MAGIC %run /Repos/sarthaks@outlook.com/adventureworks-processing/Certified/CertifiedHelperModule

# COMMAND ----------

entityName = "Address"
#Get Data from Raw layer for the Selected Entity
dfRaw = sourceRawLayerdata(entityName)

destinationPath = certifiedLandingPath+'/'+entityName
keyColumn = getKeyAttributeForGivenEntity(entityName)
print(keyColumn)
if dfRaw.count() > 0 and keyColumn is not None:
    mergedataWithTarget(incrementDataFrame=dfRaw, destinationPath=destinationPath, keyColumns=keyColumn)
    print('Data is Successfully Merged for Entity '+ entityName)
else:
    print('Data is not merged for entity ' + entityName)


# COMMAND ----------


