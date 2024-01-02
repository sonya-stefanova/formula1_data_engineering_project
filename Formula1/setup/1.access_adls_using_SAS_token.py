# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using SAS token
# MAGIC  - Set the spark config for SAS token;
# MAGIC  - List files from the demo container
# MAGIC  - read the data=> the circuits cvs file.
# MAGIC  

# COMMAND ----------

formula1_SAS_secret = dbutils.secrets.get(scope="formula1-secret-scope", key = "demo-sas-token-secret")



# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.sonyadatalakestorage.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.sonyadatalakestorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.sonyadatalakestorage.dfs.core.windows.net", formula1_SAS_secret)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@sonyadatalakestorage.dfs.core.windows.net"))


# COMMAND ----------

display(spark.read.csv("abfss://demo@sonyadatalakestorage.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


