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

qualifying_schema = StructType(fields = [
    StructField('qualifyId', IntegerType(), True),
    StructField('raceId', IntegerType(), True),
    StructField('driverId', IntegerType(), True),
    StructField('constructorid', IntegerType(), True),
    StructField('number', IntegerType(), True),
    StructField('position', StringType()),
    StructField('q1', StringType()),
    StructField('q2', StringType()),
    StructField('q3', StringType())

])

# COMMAND ----------

qualifying_df = spark.read.option('multiLine', True).json(f'{raw_folder_path}/{v_file_date}/qualifying' )

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

qualifying_renamed_df = qualifying_df.withColumnRenamed('raceId', 'race_id').withColumnRenamed('driverId', 'driver_id').withColumnRenamed('constructorId', 'constructor_id').withColumnRenamed('qualifyId', 'qualify_id')
                           

# COMMAND ----------

qualifying_final_df = add_injestion_date(qualifying_renamed_df)
qualifying_final_df = qualifying_final_df.withColumn('datasource', lit(data_source))

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table if exists f1_processed.qualifying

# COMMAND ----------

#overwrite_partition(qualifying_final_df, 'f1_processed', 'qualifying', 'race_id')

# COMMAND ----------

#qualifying_final_df.write.mode('overwrite').format('parquet').saveAsTable('f1_processed.qualifying')

# COMMAND ----------

#qualifying_final_df.write.parquet(f'{processed_folder_path}/qualifying', mode = 'overwrite')


# COMMAND ----------

#df = spark.read.parquet(f'{processed_folder_path}/qualifying')


# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id"
merge_delta_data(qualifying_final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id',)

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(*) from f1_processed.qualifying
# MAGIC group by race_id
# MAGIC order by race_id

# COMMAND ----------

dbutils.notebook.exit("Success")