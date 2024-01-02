# Databricks notebook source
# MAGIC %md
# MAGIC ##Produce Driver Standings
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

    display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col


# COMMAND ----------

constructors_standings_df = race_results_df\
    .groupBy("race_year", "team")\
    .agg(sum("points").alias("total_points"),
        count(when(col("position")==1, True)).alias("wins"))

# COMMAND ----------

display(constructors_standings_df.filter("race_year=2020"))

# COMMAND ----------

from pyspark.sql.functions import desc, rank, asc
from pyspark.sql.window import Window

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), asc('wins'))
final_df = constructors_standings_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

display(final_df.filter("race_year in (2019,2020)"))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")

# COMMAND ----------


