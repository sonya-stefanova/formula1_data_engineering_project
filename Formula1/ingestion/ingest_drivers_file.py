# Databricks notebook source
# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC ####Drivers Ingestion

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------


dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

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

drivers_df = spark.read \
.schema(drivers_schema) \
.json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

drivers_with_ingestion_date_df = add_ingestion_date(drivers_df)


# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drivers_with_columns_df = drivers_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step2. Rename columns and add new columns - done above
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step3. Drop the unwanted columns

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url"))


# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step4. Write the data into a delta file

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("formula1_processed.drivers")


# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers
# MAGIC

# COMMAND ----------


