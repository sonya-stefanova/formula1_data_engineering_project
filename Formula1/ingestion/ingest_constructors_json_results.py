# Databricks notebook source
# MAGIC %md
# MAGIC ####Results Ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step1. Read the JSON file

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

results_df = spark.read\
    .schema(results_schema)\
    .json("/mnt/sonyadatalakestorage/raw/results.json")

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

results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id")\
                                    .withColumnRenamed("raceId","race_id")\
                                    .withColumnRenamed("driverId","driver_id")\
                                    .withColumnRenamed("constructorId", "constructor_id")\
                                    .withColumnRenamed("positionText", "position_text")\
                                    .withColumnRenamed("positionOrder","position_order")\
                                    .withColumnRenamed("fastestLap","fastest_lap")\
                                    .withColumnRenamed("fastestLapTime","fastest_lap_time")\
                                    .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
                                    .withColumn("ingestion_date",current_timestamp())\
                                    


# COMMAND ----------

# MAGIC %md

# COMMAND ----------

#####Step3. Drop the unwanted columns

# COMMAND ----------

from pyspark.sql.functions import col


# COMMAND ----------

results_final_df = results_with_columns_df.drop("statusId")

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step4. Write the output the processed container as a parquet

# COMMAND ----------

results_final_df.write.mode("overwrite").partitionBy("race_id").parquet("/mnt/sonyadatalakestorage/processed/results")

# COMMAND ----------

display(spark.read.parquet("/mnt/sonyadatalakestorage/processed/results"))

# COMMAND ----------


