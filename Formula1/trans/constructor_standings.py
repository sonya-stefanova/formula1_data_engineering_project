# Databricks notebook source
# MAGIC %md
# MAGIC ##Produce Constructor Standings
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Extract the dataframe for the target date

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results") \
.filter(f"file_date = '{v_file_date}'") 

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df, 'race_year')


# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col


# COMMAND ----------

constructor_standings_df = race_results_df \
.groupBy("race_year", "team") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))


# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results") \
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import desc, rank, asc
from pyspark.sql.window import Window
constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

overwrite_partition(final_df, 'formula1_presentation', 'constructor_standings', 'race_year')


# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from formula1_presentation.constructor_standings;

# COMMAND ----------


