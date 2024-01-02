# Databricks notebook source
# MAGIC %md
# MAGIC ##Access DataFrames using SQL
# MAGIC
# MAGIC ###Objectives
# MAGIC 1. Create temporary views on dataframes;
# MAGIC 2. Access the views from the SQL cell
# MAGIC 3. Access the view from Python cell
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM v_race_results
# MAGIC WHERE race_year = 2020 

# COMMAND ----------

race_results_2020_df=spark.sql("SELECT * FROM v_race_results WHERE race_year=2020")

# COMMAND ----------

display(race_results_2020_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Global Temporary Views
# MAGIC 1. Create global temporary view on databases
# MAGIC 2. Access from an SQL cell
# MAGIC 3. Access the view from a python cell
# MAGIC 4. Access the view from another notebook
# MAGIC

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM global_temp.gv_race_results

# COMMAND ----------

spark.sql("SELECT * FROM global_temp.gv_race_results").show()

# COMMAND ----------


