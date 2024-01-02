# Databricks notebook source
# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC ####Qaulifyinf JSON Files Ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step1. Read the multiline JSON files

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                 StructField("raceId", IntegerType(), True),
                                 StructField("driverId", IntegerType(), True),
                                 StructField("constructorId", IntegerType(), True),
                                 StructField("number", IntegerType(), True),
                                 StructField("position", IntegerType(), True),
                                 StructField("q1", StringType(), True),
                                 StructField("q2", StringType(), True),
                                 StructField("q3", StringType(), True),
                                 ])

# COMMAND ----------

qualifying_df = spark.read\
    .schema(qualifying_schema)\
    .option("multiline", True)\
    .json("/mnt/sonyadatalakestorage/raw/qualifying")

# COMMAND ----------

qualifying_df.printSchema()

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df=qualifying_df.withColumnRenamed("qualifyId", "qualify_id")\
                    .withColumnRenamed("driverId", "driver_id")\
                    .withColumnRenamed("raceId", "race_id")\
                    .withColumnRenamed("constructorId", "constructor_id")\
                    .withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC #####Step4. Write the data into a parquet file

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/sonyadatalakestorage/processed/qualifying")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/sonyadatalakestorage/processed/qualifying

# COMMAND ----------

display(spark.read.parquet("dbfs:/mnt/sonyadatalakestorage/processed/qualifying"))

# COMMAND ----------


