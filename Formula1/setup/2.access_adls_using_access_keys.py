# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using keys
# MAGIC  - Select the spark config fs azure account key
# MAGIC  - List files out of the demo container
# MAGIC  - read the data=> the circuits cvs file.
# MAGIC  

# COMMAND ----------

formala1_account_key = dbutils.secrets.get(scope="formula1-secret-scope", key="formula1-acc-key")

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.sonyadatalakestorage.dfs.core.windows.net",
    formala1_account_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@sonyadatalakestorage.dfs.core.windows.net"))


# COMMAND ----------

display(spark.read.csv("abfss://demo@sonyadatalakestorage.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


