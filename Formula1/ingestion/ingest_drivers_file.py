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

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
                                 ])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                 StructField("driverRef", StringType(), True),
                                 StructField("number", IntegerType(), True),
                                 StructField("code", StringType(), True),
                                 StructField("name", name_schema),
                                 StructField("dob", DateType(), True),
                                 StructField("nationality", StringType(), True),
                                 StructField("url", StringType(), True),

                                 ])

# COMMAND ----------

drivers_df = spark.read\
    .schema(drivers_schema)\
    .json("/mnt/sonyadatalakestorage/raw/drivers.json")

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drivers_with_columns_renamed=drivers_df.withColumnRenamed("driverId", "driver_id")\
                                        .withColumnRenamed("driverRef", "driver_ref")\
                                        .withColumn("ingestion_date", current_timestamp())\
                                        .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))

# COMMAND ----------

display(drivers_with_columns_renamed)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step2. Rename columns and add new columns - done above
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step3. Drop the unwanted columns

# COMMAND ----------

drivers_final_df = drivers_with_columns_renamed.drop("url")

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step4. Write the data into a parquet file

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet("/mnt/sonyadatalakestorage/processed/drivers")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/sonyadatalakestorage/processed/drivers

# COMMAND ----------

display(spark.read.parquet("dbfs:/mnt/sonyadatalakestorage/processed/drivers"))

# COMMAND ----------


