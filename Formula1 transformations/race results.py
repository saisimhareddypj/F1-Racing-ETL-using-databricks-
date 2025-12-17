# Databricks notebook source
# MAGIC %run "../Formula1 configuration/includes"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../Formula1 configuration/common functions"

# COMMAND ----------

races_df = spark.read.format("delta").load(f'{processed_folder_path}/races')\
    .withColumnRenamed('name', 'race_name')\
    .withColumnRenamed('date', 'race_date')


# COMMAND ----------

circuits_df = spark.read.format('delta').load(f'{processed_folder_path}/circuits')\
    .withColumnRenamed('location', 'circuit_location')

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers") \
    .withColumnRenamed("name", "driver_name") \
    .withColumnRenamed("number", "driver_number") \
    .withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

constructors_df = spark.read.format("delta").load(f'{processed_folder_path}/constructors').withColumnRenamed('name', 'team')
    

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
    .filter(f"file_date == '{v_file_date}'") \
    .withColumnRenamed("time", "race_time") \
    .withColumnRenamed("race_id", "result_race_id") \
    .withColumnRenamed("file_date", "result_file_date")

# COMMAND ----------

race_circuit_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id , "inner") \
    .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

race_results_df = results_df \
    .join(race_circuit_df, results_df.result_race_id == race_circuit_df.race_id, "inner") \
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner") \
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, "inner")


# COMMAND ----------

final_df = race_results_df.select('race_id','race_year', 'race_name','circuit_location', 'driver_name', 'driver_number', 'driver_nationality','team', 'grid', 'fastest_lap','race_time', 'points','position', 'result_file_date') \
    .withColumn('created_date', current_timestamp()) \
    .withColumnRenamed("result_file_date", "file_date")

# COMMAND ----------

# df = results_df.join(races_df, results_df.result_race_id == races_df.race_id, "left") \
#                 .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "left") \
#                 .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, "left") \
#                 .join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "left") 

# COMMAND ----------

# selected_df = df.select("race_id", "race_year", "race_name", "race_date", "race_location", "driver_name", "driver_nationality", "team", "grid", "fastest_lap","race_time", "points", "position", "result_file_date")


# COMMAND ----------

# added_df = selected_df.withColumnRenamed("result_file_date", "file_date")

# COMMAND ----------

# from pyspark.sql.functions import current_timestamp

# COMMAND ----------

#final_df = added_df.withColumn('created_date' , current_timestamp())

# COMMAND ----------

#display(final_df)

# COMMAND ----------

#final_df.write.parquet(f'{presentation_folder_path}/race_results', 'overwrite')

# COMMAND ----------

#final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_presentation.race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table if exists f1_presentation.race_results

# COMMAND ----------

#overwrite_partition(final_df, 'f1_presentation', 'race_results', 'race_id')

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.race_results
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(*) from f1_presentation.race_results group by race_id order by race_id desc

# COMMAND ----------

#df = spark.read.parquet(f'{presentation_folder_path}/race_results')

# COMMAND ----------

#latest_df = df.filter("race_year = 2020 and race_name = 'Abu Dhabi Grand Prix'").orderBy(df.points.desc())

# COMMAND ----------

#display(latest_df)