# Databricks notebook source
# MAGIC %run /Repos/sarthaks@outlook.com/adventureworks-processing/Common/GlobalVaraibles

# COMMAND ----------

# import functools
# from pyspark.sql import SparkSession
# from datetime import datetime
# import traceback


# def log_function_call_and_errors(func):
#     @functools.wraps(func)
#     def wrapper(*args, **kwargs):
#         func_name = func.__name__
#         current_time = datetime.now()
#         result = None  # Initialize result variable
#         error = None

#         try:
#             # Try to execute the function and capture the result
#             result = func(*args, **kwargs)
#             log_level = "INFO"
#             log_message = f"{func_name} returned: {result}"
#         except Exception as e:
#             # Log the error and traceback
#             error = e
#             log_level = "ERROR"
#             log_message = f"Error in {func_name}: {str(e)}\n{traceback.format_exc()}"

#         # Create a log entry DataFrame
#         log_entry = [(current_time, func_name, log_level, log_message)]
#         log_df = spark.createDataFrame(log_entry, ["log_timestamp", "function_name", "log_level", "log_message"])

#         # Write the log entry to the Delta table
#         log_df.write.mode("append").format("delta").save(loggingWithErrorDeltaTablePath)

#         if log_level == "ERROR":
#             # If it's an error, raise it again to propagate the exception
#             raise error

#         return result

#     return wrapper


# COMMAND ----------

import functools
from pyspark.sql import SparkSession
from datetime import datetime

def log_function_call_and_errors(log_errors=False,layerDetails=str()):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            func_name = func.__name__
            current_time = datetime.now()

            try:
                # Execute the function
                result = func(*args, **kwargs)

                # Create a log entry for the function result
                log_entry_result = [(current_time, func_name, "INFO", f"{func_name} returned: {result}", layerDetails)]
                log_df_result = spark.createDataFrame(log_entry_result, ["log_timestamp", "function_name", "log_level", "log_message", "layer_detail"])

                # Write the log entry for the function result to the Delta table
                log_df_result.write.mode("append").option("mergeSchema", "true").format("delta").save(loggingWithErrorDeltaTablePath)

                return result
            except Exception as e:
                if log_errors:
                    # Log the error
                    log_entry_error = [(current_time, func_name, "ERROR", f"Error in {func_name}: {str(e)}", layerDetails)]
                    log_df_error = spark.createDataFrame(log_entry_error, ["log_timestamp", "function_name", "log_level", "log_message","layer_detail"])

                    # Write the log entry for the error to the Delta table
                    log_df_error.write.mode("append").option("mergeSchema", "true").format("delta").save(loggingWithErrorDeltaTablePath)

                # Re-raise the exception
                raise e

        return wrapper
    return decorator

