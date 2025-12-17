# Databricks notebook source
# MAGIC %run "../Formula1 configuration/includes"

# COMMAND ----------

# MAGIC %run "../Formula1 configuration/common functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col, desc, asc, rank
from pyspark.sql.window import Window

# COMMAND ----------

race_results_list = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_results_list, 'race_year')

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

constructor_standings_df = race_results_df \
.groupBy("race_year","team") \
.agg(sum("points").alias("total_points"),
    count(when(col("position") == 1, True)).alias("wins"))


# COMMAND ----------

constructorRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standings_df.withColumn("rank", rank().over(constructorRankSpec))

# COMMAND ----------

#constructor_standing.write.parquet(f'{presentation_folder_path}/constructor_standing', mode='overwrite')
#driver_standings.write.parquet(f'{presentation_folder_path}/driver_standings', mode='overwrite')

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists f1_presentation.constructor_standings

# COMMAND ----------

merge_condition = "tgt.race_year = src.race_year AND tgt.team = src.team "
merge_delta_data(final_df, "f1_presentation", "constructor_standings", presentation_folder_path, merge_condition, "race_year")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.constructor_standings where race_year = 2021