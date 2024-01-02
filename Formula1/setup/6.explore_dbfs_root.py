# Databricks notebook source
# MAGIC %md
# MAGIC ####Explore DBFS root
# MAGIC 1. List all the folders in the DBFS root;
# MAGIC 2. Interact with the DBFS browser;
# MAGIC 3. Upload file to the DBFS root.

# COMMAND ----------

display(dbutils.fs.ls('/'))



# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

display(spark.read.csv('/FileStore/circuits.csv'))

# COMMAND ----------


