-- Databricks notebook source
SHOW DATABASES;


-- COMMAND ----------

USE formula1_processed;

-- COMMAND ----------

SELECT CURRENT_DATABASE();



-- COMMAND ----------

SHOW TABLES;


-- COMMAND ----------

SELECT * , concat(driver_ref, '-', code) AS new_driver_ref
FROM formula1_processed.drivers;

-- COMMAND ----------

SELECT * , SPLIT(name, ' ')[0] forename, SPLIT(name, ' ') [1]surname
FROM formula1_processed.drivers;

-- COMMAND ----------

SELECT * , current_timestamp() AS ingestion_date
FROM formula1_processed.drivers;

-- COMMAND ----------

SELECT * , date_format(dob,'dd-MM-yyyy')
FROM formula1_processed.drivers;

-- COMMAND ----------

SELECT COUNT(*)
FROM formula1_processed.drivers
where nationality = "British";

-- COMMAND ----------

SELECT MAx(dob)
FROM formula1_processed.drivers;

-- COMMAND ----------

SELECT nationality, count(*) AS counter
FROM formula1_processed.drivers
GROUP BY nationality
HAVING counter>5
ORDER BY counter DESC;

-- COMMAND ----------

SELECT nationality, name, dob, RANK() OVER(PARTITION BY nationality ORDER BY dob DESC) as age_rank
FROM formula1_processed.drivers
ORDER BY nationality, age_rank

-- COMMAND ----------

USE formula1_presentation;

-- COMMAND ----------

show tables;

-- COMMAND ----------

USE formula1_presentation;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2020
AS 
SELECT race_year, driver_name, team, total_points, wins, rank
FROM driver_standings
WHERE race_year = 2020;

-- COMMAND ----------

select* from v_driver_standings_2020

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2018
AS 
SELECT race_year, driver_name, team, total_points, wins, rank
FROM driver_standings
WHERE race_year = 2018;

-- COMMAND ----------

select * from v_driver_standings_2018;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC INNER JOIN - join the records when both conditions are satisfied

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
JOIN v_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC LEFT JOIN

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
LEFT JOIN v_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name);

-- COMMAND ----------

-- MAGIC  %md
-- MAGIC RIGHT JOIN

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
RIGHT JOIN v_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name);

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
FULL JOIN v_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ANTI JOIN  - see the drivers who raced in 2018 but NOT in 2020

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 d_2018
ANTI JOIN v_driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name);

-- COMMAND ----------


