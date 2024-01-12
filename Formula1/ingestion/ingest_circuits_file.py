# Databricks notebook source
# MAGIC %md 
# MAGIC ####DATA INGESTION FOR THE CIRCUITS FILE

# COMMAND ----------

display(dbutils.fs.mounts())


# COMMAND ----------

dbutils.widgets.help()


# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source=dbutils.widgets.get("p_data_source")


# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

v_data_source

# COMMAND ----------

# MAGIC %md
# MAGIC Step1 -> Read the CSV file making use of Spark

# COMMAND ----------



# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/sonyadatalakestorage/raw
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                    StructField("circuitRef", StringType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("location", StringType(), True),
                                    StructField("country", StringType(), True),
                                    StructField("lat", DoubleType(), True),
                                    StructField("lng", DoubleType(), True),
                                    StructField("alt", IntegerType(), True),
                                    StructField("url", StringType(), True),
                                    ])

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step2 - Select only the required columns

# COMMAND ----------

from pyspark.sql.functions import col 

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step3 - Renaming the columns
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))
    

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4. Add ingestion date to the dateframe needed for audit purposes
# MAGIC

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5. Write data as parquet file.
# MAGIC

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("formula1_processed.circuits")


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/sonyadatalakestorage/processed/circuits
# MAGIC

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1_processed.circuits
