# Databricks notebook source
# MAGIC %run "../Formula1 configuration/includes"

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races')

# COMMAND ----------

races_filter_df = races_df.filter(races_df['race_year'] == 2020)

# COMMAND ----------

races_filter_df = races_df.filter("race_year == 2020")