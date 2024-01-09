# Databricks notebook source
# MAGIC %md
# MAGIC ####Results Ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step1. Read the JSON file

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                 StructField("raceId", IntegerType(), True),
                                 StructField("driverId", IntegerType(), True),
                                 StructField("constructorId", IntegerType(), True),
                                 StructField("number", IntegerType(), True),
                                 StructField("grid", IntegerType(), True),
                                 StructField("position", IntegerType(), True),
                                 StructField("positionText", StringType(), True),
                                 StructField("points", FloatType(), True),
                                 StructField("laps", IntegerType(), True),
                                 StructField("time", StringType(), True),
                                 StructField("milliseconds", IntegerType(), True),
                                 StructField("fastestLap", IntegerType(), True),
                                 StructField("rank", IntegerType(), True),
                                 StructField("fastestLapTime", StringType(), True),
                                 StructField("fastestLapSpeed", FloatType(), True),
                                 StructField("statusId", StringType(), True),
                                 ])

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step2. Rename the columns as per per the Python standard

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("file_date", lit(v_file_date))
                                    


# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

results_with_ingestion_date_df = add_ingestion_date(results_with_columns_df)

# COMMAND ----------

display(results_with_ingestion_date_df)

# COMMAND ----------

#####Step3. Drop the unwanted columns

# COMMAND ----------

from pyspark.sql.functions import col


# COMMAND ----------

results_final_df = results_with_ingestion_date_df.drop(col("statusId"))


# COMMAND ----------

# MAGIC %md
# MAGIC #####Step4. Write the output the processed container as a parquet

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE formula1_processed.results;

# COMMAND ----------

results_final_df.write.mode("overwrite").partitionBy('race_id').parquet(f"{processed_folder_path}/results")

# COMMAND ----------

overwrite_partition(results_final_df, 'formula1_processed', 'results', 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM formula1_processed.results
# MAGIC GROUP BY race_id;

# COMMAND ----------


