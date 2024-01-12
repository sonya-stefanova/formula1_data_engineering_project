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

# MAGIC %python
# MAGIC results_df=spark.read\
# MAGIC .option("inferSchema", True) \
# MAGIC .json("/mnt/sonyadatalakestorage/raw/2021-03-28/results.json")

# COMMAND ----------

# MAGIC %python
# MAGIC display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Write the data into detal file format to the datalake
# MAGIC If you want to append data = > mode should be append.

# COMMAND ----------

# MAGIC %python
# MAGIC results_df.write.format("delta").mode("overwrite").saveAsTable("formula1_demo.results_managed")
# MAGIC

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

# MAGIC %python
# MAGIC
# MAGIC results_df.write.format("delta").mode("overwrite").save("/mnt/sonyadatalakestorage/dldemo/results_external")
# MAGIC

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

# MAGIC %python
# MAGIC results_external_df = spark.read.format("delta").load("/mnt/sonyadatalakestorage/dldemo/results_external")
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC display(results_external_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Save a table through partitions

# COMMAND ----------

# MAGIC %python
# MAGIC results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("formula1_demo.results_partitioned")

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

# MAGIC %python
# MAGIC from delta.tables import *
# MAGIC from pyspark.sql.functions import *
# MAGIC
# MAGIC deltaTable = DeltaTable.forPath(spark, '/mnt/formula1dl/dldemo/results_managed')
# MAGIC
# MAGIC # Declare the predicate by using a SQL-formatted string.
# MAGIC deltaTable.delete("points = 0")
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM formula1_demo.results_managed;

# COMMAND ----------

# MAGIC %python
# MAGIC drivers_day1_df = spark.read \
# MAGIC .option("inferSchema", True) \
# MAGIC .json("/mnt/sonyadatalakestorage/raw/2021-03-28/drivers.json") \
# MAGIC .filter("driverId <= 10") \
# MAGIC .select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

# MAGIC %python
# MAGIC drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC from drivers_day1;

# COMMAND ----------

# MAGIC %python
# MAGIC drivers_day2_df = spark.read \
# MAGIC .option("inferSchema", True) \
# MAGIC .json("/mnt/sonyadatalakestorage/raw/2021-03-28/drivers.json") \
# MAGIC .filter("driverId BETWEEN 6 AND 15") \
# MAGIC .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

# MAGIC %python
# MAGIC display(drivers_day2_df)

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

# MAGIC %python
# MAGIC from pyspark.sql.functions import upper
# MAGIC
# MAGIC drivers_day3_df = spark.read \
# MAGIC .option("inferSchema", True) \
# MAGIC .json("/mnt/sonyadatalakestorage/raw/2021-03-28/drivers.json") \
# MAGIC .filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
# MAGIC .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.functions import current_timestamp
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC deltaTable = DeltaTable.forPath(spark, "/mnt/formula1dl/dldemo/drivers_merge")
# MAGIC
# MAGIC deltaTable.alias("tgt").merge(
# MAGIC     drivers_day3_df.alias("upd"),
# MAGIC     "tgt.driverId = upd.driverId") \
# MAGIC   .whenMatchedUpdate(set = { "dob" : "upd.dob", "forename" : "upd.forename", "surname" : "upd.surname", "updatedDate": "current_timestamp()" } ) \
# MAGIC   .whenNotMatchedInsert(values =
# MAGIC     {
# MAGIC       "driverId": "upd.driverId",
# MAGIC       "dob": "upd.dob",
# MAGIC       "forename" : "upd.forename", 
# MAGIC       "surname" : "upd.surname", 
# MAGIC       "createdDate": "current_timestamp()"
# MAGIC     }
# MAGIC   ) \
# MAGIC   .execute()

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

# MAGIC %sql
# MAGIC DESC HISTORY formula1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1_demo.drivers_merge VERSION AS OF 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1_demo.drivers_merge TIMESTAMP AS OF '2024-01-11T09:35:42.000+00:00';

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf", '2024-01-11T09:35:42.000+00:00').load('/mnt/formula1dl/dldemo/drivers_merge')

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Vacuum - function making data complies with the GDPR requirements
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM formula1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM formula1_demo.drivers_merge WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY formula1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formula1_demo.drivers_merge VERSION AS OF 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO formula1_demo.drivers_merge tgt
# MAGIC USING formula1_demo.drivers_merge VERSION AS OF 3 src
# MAGIC    ON (tgt.driverId = src.driverId)
# MAGIC WHEN NOT MATCHED THEN
# MAGIC    INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from formula1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history formula1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC ### TRANSACTION  LOG

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS formula1_demo.drivers_transactions (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY formula1_demo.drivers_transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO formula1_demo.drivers_transactions
# MAGIC SELECT * FROM formula1_demo.drivers_merge
# MAGIC WHERE driverId = 2;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM formula1_demo.drivers_transactions
# MAGIC WHERE driverId = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Convert PARQUET file to DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS formula1_demo.drivers_convert_to_delta (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO formula1_demo.drivers_convert_to_delta
# MAGIC SELECT * FROM formula1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA formula1_demo.drivers_convert_to_delta

# COMMAND ----------

# MAGIC %md
# MAGIC ####
# MAGIC Write a table from a delta file into a data frame and save it as a parquet

# COMMAND ----------

df = spark.table("formula1_demo.drivers_convert_to_delta")


# COMMAND ----------

df.write.format("parquet").save("/mnt/sonyadatalakestorage/dldemo/drivers_convert_to_delta_new")


# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`/mnt/sonyadatalakestorage/dldemo/drivers_convert_to_delta_new`

# COMMAND ----------


