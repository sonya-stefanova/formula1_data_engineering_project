-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####Drop All Existing Tables
-- MAGIC

-- COMMAND ----------

SHOW DATABASES;


-- COMMAND ----------

DROP DATABASE IF EXISTS formula1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS formula1_processed
LOCATION "mnt/sonyadatalakestorage/processed";

-- COMMAND ----------

DROP DATABASE IF EXISTS formula1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS formula1_presentation
LOCATION "mnt/sonyadatalakestorage/presentation";

-- COMMAND ----------


