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

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read\
    .schema(constructors_schema)\
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

constructrors_drop_df = constructors_df.drop("url")

# COMMAND ----------

display(constructrors_drop_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step3. Rename the files so to thead represent the Python-style

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df=constructrors_drop_df.withColumnRenamed("constructorId", "constructor_id")\
                                        .withColumnRenamed("constructorRef", "constructor_ref")\
                                        .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step4. Write the ingested data into a parquet file within the processed container.

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/sonyadatalakestorage/processed/constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/sonyadatalakestorage/processed/constructors

# COMMAND ----------


