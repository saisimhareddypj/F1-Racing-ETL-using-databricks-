# Databricks notebook source
# MAGIC %run "../Formula1 configuration/includes"

# COMMAND ----------

# MAGIC %run "../Formula1 configuration/common functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                     StructField("raceId", IntegerType(), True),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("constructorId", IntegerType(), True),
                                     StructField("number", IntegerType(), True),
                                     StructField("grid", IntegerType(), True),
                                     StructField("position", IntegerType(), True),
                                     StructField("positionText", StringType(), True),
                                     StructField("positionOrder", IntegerType(), True),
                                     StructField("points", FloatType(), True),
                                     StructField("laps", IntegerType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("milliseconds", IntegerType(), True),
                                     StructField("fastestLap", IntegerType(), True),
                                     StructField("rank", IntegerType(), True),
                                     StructField("fastestLapTime", StringType(), True),
                                     StructField("fastestLapSpeed", FloatType(), True),
                                     StructField("statusId", StringType(), True)])

# COMMAND ----------

results_df = spark.read.json(f'{raw_folder_path}/{v_file_date}/results.json', schema=results_schema)

# COMMAND ----------

spark.read.json(f'{raw_folder_path}/2021-03-28/results.json').createOrReplaceTempView('results_tmp')

# COMMAND ----------

# MAGIC %sql
# MAGIC select raceId, count(*) from results_tmp
# MAGIC group by raceId
# MAGIC order by raceId

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit

# COMMAND ----------

results_added_df = results_df.withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

results_selected_df = results_added_df.select(col('resultId'), col('raceId'), col('driverId'), col('constructorId'), col('grid'), col('position'), col('positionText'), col('positionOrder'), col('points'), col('laps'), col('time'), col('milliseconds'), col('fastestLap'), col('rank'), col('fastestLapTime'), col('fastestLapSpeed'), col('statusId'), col('ingestion_date'))

# COMMAND ----------

results_renamed_df = results_added_df.withColumnRenamed('resultId', 'result_id').withColumnRenamed('raceId', 'race_id').withColumnRenamed('driverId', 'driver_id').withColumnRenamed('constructorId', 'constructor_id').withColumnRenamed('positionText', 'position_text').withColumnRenamed('positionOrder', 'position_order').withColumnRenamed('fastestLap', 'fastest_lap').withColumnRenamed('fastestLapTime', 'fastest_lap_time').withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed').withColumnRenamed('statusId', 'status_id')

# COMMAND ----------

results_final_df = add_injestion_date(results_renamed_df)
results_final_df = results_renamed_df.drop('status_id')
results_final_df = results_final_df.withColumn('datasource', lit(data_source)).withColumn('file_date', lit(v_file_date))

# COMMAND ----------

results_clean_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 1 for Incremental load

# COMMAND ----------

# for race_id_list in results_final_df.select('race_id').distinct().collect(): 
#   if (spark._jsparkSession.catalog().tableExists('f1_processed.results')):
#     spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS partition (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode('append').partitionBy('race_id').format('parquet').saveAsTable('f1_processed.results')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Method 2 for Incremental load

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table if exists f1_processed.results

# COMMAND ----------

#spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

#results_final_df = results_final_df.select("result_id", "driver_id", "constructor_id", "number", "grid", "position", "position_text", "position_order", "points", "laps", "time", "milliseconds", "fastest_lap", "rank", "fastest_lap_time", "fastest_lap_speed",  "datasource", "ingestion_date", "file_date", "race_id")

# COMMAND ----------

# if spark._jsparkSession.catalog().tableExists('f1_processed.results'):
#   results_final_df.write.mode('overwrite').insertInto('f1_processed.results')
# else:
#   results_final_df.write.mode('overwrite').partitionBy('race_id').format('parquet').saveAsTable('f1_processed.results')
  
  

# COMMAND ----------

#overwrite_partition(results_final_df, 'f1_processed', 'results', 'race_id')

# COMMAND ----------

# spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning.enabled", "true")
# from delta.tables import DeltaTable

# if spark._jsparkSession.catalog().tableExists('f1_processed.results'):
#      deltaTable = DeltaTable.forPath(spark, "dbfs:/mnt/f1projectdata/processed/results")
#      deltaTable.alias("tgt").merge(
#          results_final_df.alias("src"),
#          "tgt.result_id = src.result_id and tgt.race_id = src.race_id") \
#  .whenMatchedUpdateAll() \
#  .whenNotMatchedInsertAll() \
#  .execute()
# else:
#    results_final_df.write.mode('overwrite').partitionBy('race_id').format('delta').saveAsTable('f1_processed.results')
 

# COMMAND ----------


# spark.sql("""
# SET spark.databricks.optimizer.dynamicPartitionPruning.enabled = true
# """)


# results_final_df.createOrReplaceTempView("results_final_df")


# if spark.catalog.tableExists("f1_processed.results"):

#     spark.sql("""
#     MERGE INTO f1_processed.results AS tgt
#     USING results_final_df AS src
#     ON tgt.result_id = src.result_id AND tgt.race_id = src.race_id
#     WHEN MATCHED THEN UPDATE SET *
#     WHEN NOT MATCHED THEN INSERT *
#     """)

# else:

#     spark.sql("""
#     CREATE TABLE f1_processed.results
#     USING DELTA
#     PARTITIONED BY (race_id)
#     AS SELECT * FROM results_final_df
#     """)


# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_clean_df, 
                 'f1_processed', 
                 'results',
                 processed_folder_path,
                 merge_condition,
                 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.results
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as (
# MAGIC     select  race_id, driver_id, row_number() over(partition by race_id, driver_id order by race_id, driver_id asc) as row_num
# MAGIC     from f1_processed.results
# MAGIC )
# MAGIC select * from cte where row_num > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte as(
# MAGIC   select race_id, driver_id, count(*) from f1_processed.results
# MAGIC   group by race_id, driver_id
# MAGIC   having count(*) > 1
# MAGIC   order by race_id, driver_id
# MAGIC )
# MAGIC select * from cte

# COMMAND ----------

#results_final_df.write.parquet(f'{processed_folder_path}/results', partitionBy='race_id', mode='overwrite')

# COMMAND ----------

#df = spark.read.parquet(f'{processed_folder_path}/results')

# COMMAND ----------

dbutils.notebook.exit("Success")