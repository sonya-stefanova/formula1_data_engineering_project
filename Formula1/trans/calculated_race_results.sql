-- Databricks notebook source
SHOW DATABASES;

-- COMMAND ----------

USE formula1_processed;

-- COMMAND ----------

show tables;

-- COMMAND ----------

CREATE TABLE formula1_presentation.calculated_race_results
USING parquet
AS
SELECT races.race_year, 
       constructors.name AS team_name,
       drivers.name AS driver_name,
       output.points,
       output.position,
       11-output.position AS calculated_points
FROM output
JOIN formula1_processed.drivers 
ON (output.driver_id = drivers.driver_id)
JOIN formula1_processed.constructors 
ON (output.constructor_id = constructors.constructor_id)
JOIN formula1_processed.races
ON (output.race_id = races.race_id)
WHERE output.position <=10

-- COMMAND ----------

SELECT * FROM formula1_presentation.calculated_race_results

-- COMMAND ----------


