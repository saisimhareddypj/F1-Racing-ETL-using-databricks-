# Databricks notebook source
# MAGIC %run "../Formula1 configuration/includes"

# COMMAND ----------

race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_race_results
# MAGIC where race_year = 2019
# MAGIC

# COMMAND ----------

df = spark.sql("select * from v_race_results where race_year = 2020")

# COMMAND ----------

display(df)

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp

# COMMAND ----------

df = spark.sql("select * from global_temp.gv_race_results where race_year = 2018")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results
# MAGIC where race_year = 2019