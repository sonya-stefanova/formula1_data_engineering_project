# Databricks notebook source
# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC ####Drivers Ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step1. Read the JSON file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                 StructField("driverId", IntegerType(), True),
                                 StructField("stop", StringType(), True),
                                 StructField("lap", IntegerType(), True),
                                 StructField("time", StringType(), True),
                                 StructField("duration", StringType(), True),
                                 StructField("milliseconds", IntegerType(), True),
                                 ])

# COMMAND ----------

pit_stops_df = spark.read\
    .schema(pit_stops_schema)\
    .option("multiline", True)\
    .json("/mnt/sonyadatalakestorage/raw/pit_stops.json")

# COMMAND ----------

pit_stops_df.printSchema()

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df=pit_stops_df.withColumnRenamed("racerId", "racer_id")\
                    .withColumnRenamed("driverId", "driver_id")\
                    .withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC #####Step4. Write the data into a parquet file

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/sonyadatalakestorage/processed/pit_stops")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/sonyadatalakestorage/processed/pit_stops

# COMMAND ----------

display(spark.read.parquet("dbfs:/mnt/sonyadatalakestorage/processed/pit_stops"))

# COMMAND ----------


