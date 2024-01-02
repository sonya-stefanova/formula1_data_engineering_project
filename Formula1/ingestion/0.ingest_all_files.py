# Databricks notebook source
v_result = dbutils.notebook.run("ingest_circuits_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------


