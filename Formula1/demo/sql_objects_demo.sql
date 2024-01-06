-- Databricks notebook source
-- MAGIC %md
-- MAGIC 1. Spark SQL Documentation checked - https://spark.apache.org/docs/latest/sql-ref.html
-- MAGIC 2. Create Database demo
-- MAGIC 3. Data tab in the UI
-- MAGIC 4. SHOW command
-- MAGIC 5. DESCRIBE command
-- MAGIC 6. Find the current database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

show databases

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##MANAGED TABLES

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------

desc extended race_results_python;


-- COMMAND ----------

select * FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

CREATE TABLE demo.race_results_sql
AS
select * FROM demo.race_results_python
WHERE race_year = 2020;


-- COMMAND ----------

DROP TABLE demo.race_results_sql;


-- COMMAND ----------

SELECT current_database();


-- COMMAND ----------

SHOW TABLES in demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create External Tables by means of Python and SQL

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(race_year INT,
race_name STRING, 
race_date TIMESTAMP,
circuit_location STRING,
driver_name STRING,
driver_number INT, 
driver_nationality STRING,
team STRING,
grid INT, 
fastest_lap INT, 
race_time STRING,
points FLOAT, 
position INT, 
created_date TIMESTAMP
)
USING parquet
LOCATION "/mnt/sonyadatalakestorage/presentation/demo.race_results_ext_sql"

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py 
WHERE race_year = 2020;


-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql

-- COMMAND ----------

SHOW TABLES IN demo;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## WORKING WITH VIEWS ON TABLES

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS
SELECT * 
FROM demo.race_results_python
WHERE race_year = 2020;


-- COMMAND ----------

SELECT *
FROM v_race_results;


-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS
SELECT * 
FROM demo.race_results_python
WHERE race_year = 2015;

-- COMMAND ----------

SELECT *
FROM global_temp.gv_race_results;

-- COMMAND ----------

show tables in global_temp

-- COMMAND ----------

CREATE
