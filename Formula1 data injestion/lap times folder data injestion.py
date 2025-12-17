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

lap_times_schema = StructType(fields=[StructField('raceId', IntegerType(), False),
                                      StructField('driverId', IntegerType(), True),
                                      StructField('stop', StringType(), True),
                                      StructField('lap', IntegerType(), True),
                                      StructField('time', StringType(), True),
                                      StructField('duration', StringType(), True),
                                      StructField('milliseconds', IntegerType(), True)])

# COMMAND ----------

lap_times_df = spark.read.csv(f'{raw_folder_path}/{v_file_date}/lap_times', header=True, schema=lap_times_schema)

# COMMAND ----------

lap_times_df.printSchema()

# COMMAND ----------

lap_times_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

lap_times_renamed_df = lap_times_df.withColumnRenamed('raceId', 'race_id') \
                           .withColumnRenamed('driverId', 'driver_id') \
                           

# COMMAND ----------

lap_times_final_df = add_injestion_date(lap_times_renamed_df)
lap_times_final_df = lap_times_final_df.withColumn("datasource", lit(data_source))

# COMMAND ----------

#lap_times_clean_df = lap_times_final_df.dropDuplicates(['race_id', 'driver_id', 'stop'])

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table if exists f1_processed.lap_times

# COMMAND ----------

#overwrite_partition(lap_times_final_df, 'f1_processed', 'lap_times', 'race_id')

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap"
merge_delta_data(lap_times_final_df, 'f1_processed', 'lap_times', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, driver_id, lap, stop, count(*) from f1_processed.lap_times
# MAGIC group by race_id, driver_id, lap, stop
# MAGIC having count(*) > 1
# MAGIC order by race_id desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.lap_times
# MAGIC where race_id = 1053 AND driver_id = 20 AND lap = 16
# MAGIC     
# MAGIC

# COMMAND ----------

#lap_times_final_df.write.parquet(f'{processed_folder_path}/lap_times', mode = 'overwrite')


# COMMAND ----------

#lap_times_final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.lap_times')

# COMMAND ----------

#df = spark.read.parquet(f'{processed_folder_path}/lap_times')


# COMMAND ----------

#display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")