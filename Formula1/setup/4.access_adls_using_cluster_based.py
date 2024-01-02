# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using cluster-based credentials
# MAGIC  - Select the spark config fs azure account key
# MAGIC  - List files out of the demo container
# MAGIC  - read the data from the circuits cvs file.
# MAGIC  

# COMMAND ----------

# spark.conf.set(
#     "fs.azure.account.key.sonyadatalakestorage.dfs.core.windows.net",
#     "VSs2J/H7cjYCUizzR3ZznP3e6lvXzflKtMOvGNHVXQOaWxxGp+OUX/+DWwBuy/r3n81EDP9kQ5PL+AStO3sXbg=="
# )

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@sonyadatalakestorage.dfs.core.windows.net"))


# COMMAND ----------

display(spark.read.csv("abfss://demo@sonyadatalakestorage.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


