-- Databricks notebook source
USE formula1_presentation;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
SELECT driver_name,
      COUNT(1) AS total_races,
      sum(calculated_points) AS total_points,
      AVG(calculated_points) AS avg_points,
      RANK() OVER(ORDER BY AVG(calculated_points) DESC) driver_rank
FROM formula1_presentation.calculated_race_results
GROUP BY driver_name
HAVING count(1)>=50
ORDER BY avg_points DESC;


-- COMMAND ----------

SELECT race_year,
      driver_name,
      COUNT(1) AS total_races,
      sum(calculated_points) AS total_points,
      AVG(calculated_points) AS avg_points
FROM formula1_presentation.calculated_race_results
WHERE driver_name in (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank<=10)
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC; 

-- COMMAND ----------

SELECT race_year,
      driver_name,
      COUNT(1) AS total_races,
      sum(calculated_points) AS total_points,
      AVG(calculated_points) AS avg_points
FROM formula1_presentation.calculated_race_results
WHERE driver_name in (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank<=10)
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC;

-- COMMAND ----------

SELECT race_year,
      driver_name,
      COUNT(1) AS total_races,
      sum(calculated_points) AS total_points,
      AVG(calculated_points) AS avg_points
FROM formula1_presentation.calculated_race_results
WHERE driver_name in (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank<=10)
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC;
