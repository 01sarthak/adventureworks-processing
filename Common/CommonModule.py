# Databricks notebook source
# MAGIC %run /Repos/sarthaks@outlook.com/adventureworks-processing/Common/LoggingWithError

# COMMAND ----------

from delta.tables import *

@log_function_call_and_errors
def MountStorage(mount_name, container_name):
    if all((mount.mountPoint != mount_name and  container_name+'@' not in mount.source) for mount in dbutils.fs.mounts()):
        configs = {"fs.azure.account.auth.type": "OAuth",
            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="Storage-Connect",key="application-id"),
            "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="Storage-Connect",key="service-credential-key-name"),
            "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/7e1d886a-38be-44e3-b830-42a4c1575af5/oauth2/token"}

        # Optionally, you can add <directory-name> to the source URI of your mount point.
        dbutils.fs.mount(
        source = f"abfss://{container_name}@storagedevsarthak2023.dfs.core.windows.net/",
        mount_point = mount_name,
        extra_configs = configs)

@log_function_call_and_errors
def deleteDeltaTableAll(path):
    # Load the Delta table
    delta_table = DeltaTable.forPath(spark, path)

    # Delete all data from the Delta table (no condition specified)
    delta_table.delete()
    return True

@log_function_call_and_errors
def deleteDeltaTableWithCondition(path, condition):
    # Load the Delta table
    delta_table = DeltaTable.forPath(spark, path)

    # Delete all data from the Delta table (no condition specified)
    delta_table.delete(condition)
    return True


# COMMAND ----------


dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
