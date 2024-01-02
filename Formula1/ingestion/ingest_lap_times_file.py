# Databricks notebook source
# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC ####Ingestion of the Lap Times Folder

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step1. Read the CSV files

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                 StructField("driverId", IntegerType(), True),
                                 StructField("lap", IntegerType(), True),
                                 StructField("position", IntegerType(), True),
                                 StructField("time", StringType(), True),
                                 StructField("milliseconds", IntegerType(), True),
                                 ])

# COMMAND ----------

lap_times_df = spark.read\
    .schema(lap_times_schema)\
    .csv("/mnt/sonyadatalakestorage/raw/lap_times")

# COMMAND ----------

lap_times_df.printSchema()

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

lap_times_df.count()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df=lap_times_df.withColumnRenamed("racerId", "racer_id")\
                    .withColumnRenamed("driverId", "driver_id")\
                    .withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC #####Step4. Write the data into a parquet file

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/sonyadatalakestorage/processed/lap_times")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/sonyadatalakestorage/processed/lap_times

# COMMAND ----------

display(spark.read.parquet("dbfs:/mnt/sonyadatalakestorage/processed/lap_times"))

# COMMAND ----------


