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

# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")


# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------



# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"


# COMMAND ----------

constructor_df = spark.read \
.schema(constructors_schema) \
.json("/mnt/sonyadatalakestorage/raw/constructors.json")

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step2.Drop unwanted columns
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col


# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

display(constructor_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step3. Rename the files so to thead represent the Python-style

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

from pyspark.sql.functions import lit


# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step4. Write the ingested data into a parquet file within the processed container.

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("formula1_processed.constructors")

# COMMAND ----------

display(spark.read.parquet("/mnt/sonyadatalakestorage/processed/constructors"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1_processed.constructors
# MAGIC

# COMMAND ----------

dbutils.notebook.exit("Success")
