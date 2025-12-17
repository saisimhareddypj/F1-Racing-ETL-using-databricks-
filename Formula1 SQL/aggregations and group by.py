# Databricks notebook source
# MAGIC %run "../Formula1 configuration/includes"

# COMMAND ----------

race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

demo_df = race_results_df.filter("race_year = 2020")

# COMMAND ----------

display(demo_df)


# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col, desc, rank, countDistinct

# COMMAND ----------

demo_df.select(count('*')).show()

# COMMAND ----------

demo_df.select(countDistinct('race_name')).show()

# COMMAND ----------

demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum('points'), countDistinct("race_name"))\
    .withColumnRenamed('sum(points)', 'total points').withColumnRenamed('count(DISTINCT race_name)', 'number of races')\
    .show()

# COMMAND ----------

demo_df.groupBy('driver_name').agg(sum('points'), countDistinct("race_name"))\
    .withColumnRenamed('sum(points)', 'total points')\
    .withColumnRenamed('count(DISTINCT race_name)', 'number of races')\
    .show()
    

# COMMAND ----------

new_demo_df = race_results_df.filter("race_year in(2019, 2020)")

# COMMAND ----------

driver_rank_df = new_demo_df.groupBy("race_year", "driver_name")\
.agg(sum('points').alias ('total points'), countDistinct("race_name").alias ('number of races'))

# COMMAND ----------

river_rank_spec = Window.partitionBy("race_year").orderBy(desc("total points"))
driver_rank_df = driver_rank_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc


# COMMAND ----------

driver_rank_df = new_demo_df.groupBy("race_year", "driver_name")\
.agg(sum('points').alias ('total points'), countDistinct("race_name").alias ('number of races'))
driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total points"))
driver_rank_df = driver_rank_df.withColumn("rank", rank().over(driver_rank_spec))
   

# COMMAND ----------

display(driver_rank_df)