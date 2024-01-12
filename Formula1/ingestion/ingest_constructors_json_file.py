# Databricks notebook source
# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC ####Ingesting Constructors JSON file(s)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step.1 Read the JSON file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")


# COMMAND ----------

# MAGIC %run "../includes/common_functions"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
.schema(constructors_schema) \
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step2.Drop unwanted columns
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, lit


# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step3. Rename the files so to thead represent the Python-style

# COMMAND ----------

constructor_renamed_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("data_source", lit(v_data_source)) \
                                             .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_renamed_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #####Step4. Write the ingested data into a DELTA file within the processed container.

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("formula1_processed.constructors")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1_processed.constructors

# COMMAND ----------

dbutils.notebook.exit("Success")
