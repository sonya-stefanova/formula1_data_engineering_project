# Databricks notebook source
# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC ####PitStops Ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step1. Read the JSON file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

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

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")


# COMMAND ----------

pit_stops_with_ingestion_date_df = add_ingestion_date(pit_stops_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

final_df = pit_stops_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))


# COMMAND ----------

# MAGIC %md
# MAGIC #####Step4. Write the data into a parquet file

# COMMAND ----------

overwrite_partition(final_df, 'formula1_processed', 'pit_stops', 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM formula1_processed.pit_stops;

# COMMAND ----------


