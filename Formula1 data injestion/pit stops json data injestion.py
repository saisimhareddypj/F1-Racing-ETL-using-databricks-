# Databricks notebook source
# MAGIC %run "../Formula1 configuration/includes"

# COMMAND ----------

# MAGIC %run "../Formula1 configuration/common functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField('raceId', IntegerType(), False),
                                      StructField('driverId', IntegerType(), True),
                                      StructField('stop', StringType(), True),
                                      StructField('lap', IntegerType(), True),
                                      StructField('time', StringType(), True),
                                      StructField('duration', StringType(), True),
                                      StructField('milliseconds', IntegerType(), True)])

# COMMAND ----------

pit_stops_df = spark.read.option('multiLine', True).json(f'{raw_folder_path}/{v_file_date}/pit_stops.json', schema = pit_stops_schema)

# COMMAND ----------

pit_stops_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

pit_stops_renamed_df = pit_stops_df.withColumnRenamed('raceId', 'race_id') \
                           .withColumnRenamed('driverId', 'driver_id') \
                           

# COMMAND ----------

pit_stops_final_df = add_injestion_date(pit_stops_renamed_df)
pit_stops_final_df = pit_stops_final_df.withColumn('datasource', lit(data_source))


# COMMAND ----------

#pit_stops_clean_df = pit_stops_final_df.dropDuplicates(['race_id', 'driver_id', 'stop'])

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table if exists f1_processed.pit_stops;

# COMMAND ----------

#overwrite_partition(pit_stops_final_df, 'f1_processed', 'pit_stops', 'race_id')

# COMMAND ----------

merge_condition = 'tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id AND tgt.stop = src.stop'
merge_delta_data(pit_stops_final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(*) from f1_processed.pit_stops
# MAGIC group by race_id
# MAGIC order by race_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, driver_id, stop, count(*) from f1_processed.pit_stops
# MAGIC group by race_id, driver_id, stop
# MAGIC having count(*) > 1
# MAGIC order by race_id

# COMMAND ----------

#pit_stops_final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.pit_stops')

# COMMAND ----------

#pit_stops_final_df.write.parquet(f'{processed_folder_path}/pit_stops', mode = 'overwrite')


# COMMAND ----------

#df = spark.read.parquet(f'{processed_folder_path}/pit_stops')


# COMMAND ----------

dbutils.notebook.exit("Success")