# Databricks notebook source
# MAGIC %md
# MAGIC ####Demo tasks: 
# MAGIC 1. Write data to delta lake (managed table)
# MAGIC 2. Write data to delta lake (external table)
# MAGIC 3. Read data from delta lake (Table)
# MAGIC 4. Read data from delta lake (File)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS formula1_demo
# MAGIC LOCATION '/mnt/formula1dl/dldemo'

# COMMAND ----------

results_df=spark.read\
.option("inferSchema", True) \
.json("/mnt/sonyadatalakestorage/raw/2021-03-28/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Write the data into detal file format to the datalake
# MAGIC If you want to append data = > mode should be append.

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("formula1_demo.results_managed")


# COMMAND ----------

# MAGIC %md
# MAGIC #####Read the delta files

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC Write data to a file location

# COMMAND ----------


results_df.write.format("delta").mode("overwrite").save("/mnt/sonyadatalakestorage/dldemo/results_external")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE formula1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sonyadatalakestorage/dldemo/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1_demo.results_external
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####If you want to read the file directly instead from a table by using spark - load!
# MAGIC

# COMMAND ----------

results_external_df = spark.read.format("delta").load("/mnt/sonyadatalakestorage/dldemo/results_external")


# COMMAND ----------

display(results_external_df)

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("formula1_demo.results_partitioned")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Save a table through partitions

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("formula1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS formula1_demo.results_partitioned
