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

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read\
    .schema(constructors_schema)\
    .json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step2.Drop unwanted columns
# MAGIC

# COMMAND ----------

constructrors_drop_df = constructors_df.drop("url")

# COMMAND ----------

display(constructrors_drop_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step3. Rename the files so to thead represent the Python-style

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructor_final_df=constructrors_drop_df.withColumnRenamed("constructorId", "constructor_id")\
                                        .withColumnRenamed("constructorRef", "constructor_ref")\
                                        .withColumn("ingestion_date", current_timestamp())\
                                        .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step4. Write the ingested data into a parquet file within the processed container.

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE formula1_processed.drivers;

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("formula1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")
