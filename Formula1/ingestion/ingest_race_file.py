# Databricks notebook source
# MAGIC %md
# MAGIC ####DATA INGESTION FOR THE RACE FILE

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 1. Read the CSV row file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True)
])

# COMMAND ----------

race_df = spark.read\
.option("header", True)\
.schema(races_schema)\
.csv("/mnt/sonyadatalakestorage/raw/races.csv")

# COMMAND ----------

display(race_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step2. Add extra columns => race_timestamp and ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

races_with_timestamp_df = race_df.withColumn("ingestion_date", current_timestamp())\
    .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

display(races_with_timestamp_df)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #####Step3. Select the needed columns and rename them accordingly

# COMMAND ----------

selected_races_df = races_with_timestamp_df.select(col("raceId").alias("race_id"), col("year").alias("race_year"), col("round"), col("circuitId").alias("circuit_id"), col("name"), col("ingestion_date"), col("race_timestamp"))

# COMMAND ----------

 display(selected_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step4.Write the output the processed container in a parquet format. 

# COMMAND ----------

selected_races_df.write.mode("overwrite").partitionBy("race_year").parquet("/mnt/sonyadatalakestorage/processed/races")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/sonyadatalakestorage/processed/races

# COMMAND ----------

display(spark.read.parquet("/mnt/sonyadatalakestorage/processed/races"))

# COMMAND ----------


