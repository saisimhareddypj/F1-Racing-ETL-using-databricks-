# Databricks notebook source
# MAGIC %run "../Formula1 configuration/includes"

# COMMAND ----------

circuits_df = spark.read.parquet(f'{processed_folder_path}/circuits')\
    .withColumnRenamed('name', 'circuit_name')
display(circuits_df)

# COMMAND ----------

races_df = spark.read.parquet(f'{processed_folder_path}/races')\
    .withColumnRenamed('name', 'race_name')
display(races_df)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, 'inner').select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

short_circuits_df = circuits_df.filter("circuit_id <= 50")
display(short_circuits_df)

# COMMAND ----------

race_circuits_left_df = short_circuits_df.join(races_df, short_circuits_df.circuit_id == races_df.circuit_id, 'left').select(short_circuits_df.circuit_name, short_circuits_df.location, short_circuits_df.country, races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_left_df)

# COMMAND ----------

race_circuits_right_df = short_circuits_df.join(races_df, short_circuits_df.circuit_id == races_df.circuit_id, 'right').select(short_circuits_df.circuit_name, short_circuits_df.location, short_circuits_df.country, races_df.race_name,races_df.round)

# COMMAND ----------

display(race_circuits_right_df)

# COMMAND ----------

race_circuits_semi_df = short_circuits_df.join(races_df, short_circuits_df.circuit_id == races_df.circuit_id, 'semi')

# COMMAND ----------

display(race_circuits_semi_df)

# COMMAND ----------

race_circuits_anti_df = short_circuits_df.join(races_df, short_circuits_df.circuit_id == races_df.circuit_id, 'anti')

# COMMAND ----------

display(race_circuits_anti_df)

# COMMAND ----------

race_circuits_cross_df = races_df.crossJoin(circuits_df)
display(race_circuits_cross_df)