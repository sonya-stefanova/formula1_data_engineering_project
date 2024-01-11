# Databricks notebook source
# MAGIC %md
# MAGIC ####Demo tasks: 
# MAGIC 1. Write data to delta lake (managed table)
# MAGIC 2. Write data to delta lake (external table)
# MAGIC 3. Read data from delta lake (Table)
# MAGIC 4. Read data from delta lake (File)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS formula1_demo
# MAGIC LOCATION '/mnt/formula1dl/dldemo'

# COMMAND ----------

results_df=spark.read\
.option("inferSchema", True) \
.json("/mnt/sonyadatalakestorage/raw/2021-03-28/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Write the data into detal file format to the datalake
# MAGIC If you want to append data = > mode should be append.

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("formula1_demo.results_managed")


# COMMAND ----------

# MAGIC %md
# MAGIC #####Read the delta files

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC Write data to a file location

# COMMAND ----------


results_df.write.format("delta").mode("overwrite").save("/mnt/sonyadatalakestorage/dldemo/results_external")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE if not EXISTS formula1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/sonyadatalakestorage/dldemo/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1_demo.results_external
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####If you want to read the file directly instead from a table by using spark - load!
# MAGIC

# COMMAND ----------

results_external_df = spark.read.format("delta").load("/mnt/sonyadatalakestorage/dldemo/results_external")


# COMMAND ----------

display(results_external_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save a table through partitions

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("formula1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS formula1_demo.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC ####Update and delete table
# MAGIC /Delta lake supports updates, deletes, and merges/

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE formula1_demo.results_managed
# MAGIC SET points = 11 - position
# MAGIC WHERE position<=10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM formula1_demo.results_managed
# MAGIC where position>10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM formula1_demo.results_managed;

# COMMAND ----------

from delta.tables import *
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, '/mnt/formula1dl/dldemo/results_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.delete("points = 0")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM formula1_demo.results_managed;

# COMMAND ----------

drivers_day1_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/sonyadatalakestorage/raw/2021-03-28/drivers.json") \
.filter("driverId <= 10") \
.select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC from drivers_day1;

# COMMAND ----------

drivers_day2_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/sonyadatalakestorage/raw/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 6 AND 15") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))


# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CURRENT_DATABASE()
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC use formula1_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CURRENT_DATABASE()
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS formula1_demo.drivers_merge (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC ###Day1
# MAGIC

# COMMAND ----------

# MAGIC  %sql
# MAGIC  MERGE INTO formula1_demo.drivers_merge
# MAGIC  USING drivers_day1
# MAGIC  ON formula1_demo.drivers_merge.driverId = drivers_day1.driverId
# MAGIC  WHEN MATCHED THEN
# MAGIC    UPDATE SET formula1_demo.drivers_merge.dob = drivers_day1.dob,
# MAGIC               formula1_demo.drivers_merge.forename = drivers_day1.forename,
# MAGIC               formula1_demo.drivers_merge.surname = drivers_day1.surname,
# MAGIC               formula1_demo.drivers_merge.updatedDate = current_timestamp
# MAGIC  WHEN NOT MATCHED
# MAGIC    THEN INSERT (driverId, dob, forename,surname,createdDate ) VALUES (driverId, dob, forename,surname, current_timestamp)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT*
# MAGIC from formula1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC DAY 2
# MAGIC

# COMMAND ----------

# MAGIC  %sql
# MAGIC  MERGE INTO formula1_demo.drivers_merge
# MAGIC  USING drivers_day2
# MAGIC  ON formula1_demo.drivers_merge.driverId = drivers_day2.driverId
# MAGIC  WHEN MATCHED THEN
# MAGIC    UPDATE SET formula1_demo.drivers_merge.dob = drivers_day2.dob,
# MAGIC               formula1_demo.drivers_merge.forename = drivers_day2.forename,
# MAGIC               formula1_demo.drivers_merge.surname = drivers_day2.surname,
# MAGIC               formula1_demo.drivers_merge.updatedDate = current_timestamp
# MAGIC  WHEN NOT MATCHED
# MAGIC    THEN INSERT (driverId, dob, forename,surname,createdDate ) VALUES (driverId, dob, forename,surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT*
# MAGIC from formula1_demo.drivers_merge;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Day 3

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day3_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/sonyadatalakestorage/raw/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))


# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dl/dldemo/drivers_merge")

deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId") \
  .whenMatchedUpdate(set = { "dob" : "upd.dob", "forename" : "upd.forename", "surname" : "upd.surname", "updatedDate": "current_timestamp()" } ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "upd.driverId",
      "dob": "upd.dob",
      "forename" : "upd.forename", 
      "surname" : "upd.surname", 
      "createdDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT*
# MAGIC from formula1_demo.drivers_merge;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Advanced functions in delta lake
# MAGIC
# MAGIC ####Understanding of history;
# MAGIC ####Understanding of time travelling;
# MAGIC ####Understanding of vacuum

# COMMAND ----------


