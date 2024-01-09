# Databricks notebook source
# MAGIC %md
# MAGIC ####DATA INGESTION FOR THE RACE FILE

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 1. Read the CSV row file

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

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
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

display(race_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step2. Add extra columns => race_timestamp and ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

races_with_timestamp_df = race_df.withColumn("ingestion_date", current_timestamp())\
    .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))\
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #####Step3. Select the needed columns and rename them accordingly

# COMMAND ----------

selected_races_df = races_with_timestamp_df.select(col("raceId").alias("race_id"), col("year").alias("race_year"), col("round"), col("circuitId").alias("circuit_id"), col("name"), col("ingestion_date"), col("race_timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step4.Write the output the processed container in a parquet format. 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE formula1_processed.races;

# COMMAND ----------

selected_races_df.write.mode('overwrite').partitionBy('race_year').parquet(f'{processed_folder_path}/races')

# COMMAND ----------

overwrite_partition(selected_races_df, 'formula1_processed', 'races', 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")
