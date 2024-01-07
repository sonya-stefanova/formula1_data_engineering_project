-- Databricks notebook source
-- MAGIC %python
-- MAGIC html_h1 = """<h1 style="color:Red;font-family:Arial;text-align:center">Report On Dominant Teams</h1>"""
-- MAGIC displayHTML(html_h1)

-- COMMAND ----------

USE formula1_presentation;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
SELECT team_name,
      COUNT(1) AS total_races,
      sum(calculated_points) AS total_points,
      AVG(calculated_points) AS avg_points,
      RANK() OVER(ORDER BY AVG(calculated_points) DESC) team_rank
FROM formula1_presentation.calculated_race_results
GROUP BY team_name
HAVING count(1)>=100
ORDER BY avg_points DESC;


-- COMMAND ----------

select * from v_dominant_teams;

-- COMMAND ----------

SELECT race_year,
      team_name,
      COUNT(1) AS total_races,
      sum(calculated_points) AS total_points,
      AVG(calculated_points) AS avg_points
FROM formula1_presentation.calculated_race_results
WHERE team_name in (SELECT team_name FROM v_dominant_teams WHERE team_rank<=10)
GROUP BY race_year, team_name
ORDER BY race_year, avg_points DESC; 

-- COMMAND ----------

SELECT race_year,
      team_name,
      COUNT(1) AS total_races,
      sum(calculated_points) AS total_points,
      AVG(calculated_points) AS avg_points
FROM formula1_presentation.calculated_race_results
WHERE team_name in (SELECT team_name FROM v_dominant_teams WHERE team_rank<=5)
GROUP BY race_year, team_name
ORDER BY race_year, avg_points DESC;

-- COMMAND ----------

SELECT race_year,
      team_name,
      COUNT(1) AS total_races,
      sum(calculated_points) AS total_points,
      AVG(calculated_points) AS avg_points
FROM formula1_presentation.calculated_race_results
WHERE team_name in (SELECT team_name FROM v_dominant_teams WHERE team_rank<=10)
GROUP BY race_year, team_name
ORDER BY race_year, avg_points DESC;
