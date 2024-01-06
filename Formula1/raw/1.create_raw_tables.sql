-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS formula1_raw;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create circuits table

-- COMMAND ----------

DROP TABLE IF EXISTS formula1_raw.circuits;
create table if not exists formula1_raw.circuits(circuitsId  INT,
  circuitRef STRING,
  name STRING, 
  location STRING, 
  lat DOUBLE, 
  lng DOUBLE, 
  alt INT, 
  url STRING
)
USING csv
OPTIONS (path "/mnt/sonyadatalakestorage/raw/circuits.csv", header true)

-- COMMAND ----------

SELECT * FROM formula1_raw.circuits

-- COMMAND ----------

DROP TABLE IF EXISTS formula1_raw.races;
CREATE TABLE IF NOT EXISTS formula1_raw.races(raceID INT, 
year INT,
round INT,
circuits INT, 
name STRING, 
date DATE, 
time STRING, 
url STRING
)
USING CSV
OPTIONS (path "/mnt/sonyadatalakestorage/raw/races.csv", header true)

-- COMMAND ----------

SELECT *FROM formula1_raw.races


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Create tables from JSON files
-- MAGIC 1. Create Constructors Table;
-- MAGIC 2. Create Races Table;
-- MAGIC 3. Create Results Table;

-- COMMAND ----------

DROP TABLE IF EXISTS formula1_raw.constructors;
CREATE TABLE IF NOT EXISTS formula1_raw.constructors(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING, 
  url STRING
)
USING json
OPTIONS (path "/mnt/sonyadatalakestorage/raw/constructors.json")

-- COMMAND ----------

DROP TABLE IF EXISTS formula1_raw.drivers;
CREATE TABLE IF NOT EXISTS formula1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING, 
  name STRUCT<forename: STRING, surname:STRING>,
  dob DATE, 
  nationality STRING, 
  url STRING
)
USING json
OPTIONS (path "/mnt/sonyadatalakestorage/raw/drivers.json")

-- COMMAND ----------

SELECT * FROM formula1_raw.drivers

-- COMMAND ----------

DROP TABLE IF EXISTS formula1_raw.results;
CREATE TABLE IF NOT EXISTS formula1_raw.results(
  resultsId INT, 
  raceId INT,
  driverId INT, 
  constructorId INT,
  number INT, 
  grid INT,
  position INT,
  positionText INT, 
  positionOrder INT,
  points INT, 
  laps INT, 
  time STRING, 
  milliseconds INT, 
  fastestLap INT, 
  rank INT, 
  fastestLapTime STRING, 
  fastestLapSpeed FLOAT,
  statusId INT
)
USING json
OPTIONS (path "/mnt/sonyadatalakestorage/raw/results.json")

-- COMMAND ----------

SELECT * FROM formula1_raw.results

-- COMMAND ----------

DROP TABLE IF EXISTS formula1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS formula1_raw.pit_stops(
  driverId INT, 
  duration STRING, 
  lap INT, 
  milliseconds INT, 
  raceId INT, 
  stop INT, 
  time STRING
)
USING json
OPTIONS (path "/mnt/sonyadatalakestorage/raw/pit_stops.json", multiline true)

-- COMMAND ----------

SELECT * FROM formula1_raw.pit_stops

-- COMMAND ----------

DROP TABLE IF EXISTS formula1_raw.lap_times;
CREATE TABLE IF NOT EXISTS formula1_raw.lap_times(
raceId  INT, 
driverId INT,
lap INT, 
position INT, 
time STRING, 
milliseconds INT
)
USING csv
OPTIONS (path "/mnt/sonyadatalakestorage/raw/lap_times")

-- COMMAND ----------

SELECT * FROM formula1_raw.lap_times

-- COMMAND ----------

DROP TABLE IF EXISTS formula1_raw.qualifying;
CREATE TABLE IF NOT EXISTS formula1_raw.qualifying(
qualifyId INT, 
raceId INT, 
driverId INT, 
constructorId INT, 
number INT, 
position INT, 
q1 STRING, 
q2 STRING, 
q3 STRING
)
USING json
OPTIONS (path "/mnt/sonyadatalakestorage/raw/qualifying", multiLine true)

-- COMMAND ----------

SELECT * FROM formula1_raw.qualifying
