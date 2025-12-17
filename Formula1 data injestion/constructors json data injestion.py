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

constructor_schema = "constructorId INTEGER, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df=spark.read.json(f'{raw_folder_path}/{v_file_date}/constructors.json', schema=constructor_schema)

# COMMAND ----------

constructor_drop_df=constructor_df.drop(constructor_df.url)
display(constructor_drop_df)

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp


# COMMAND ----------

constructor_renamed_df=constructor_drop_df.withColumnRenamed('constructorId','constructor_id').withColumnRenamed('constructorRef','constructor_ref')

# COMMAND ----------

constructor_final_df = add_injestion_date(constructor_renamed_df)

# COMMAND ----------

constructor_final_df = constructor_final_df.withColumn("datasource", lit(data_source))\
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table if exists f1_processed.constructor

# COMMAND ----------

#overwrite_partition(constructor_final_df, 'f1_processed', 'constructor', 'constructor_id')

# COMMAND ----------

constructor_final_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.constructors')

# COMMAND ----------

# MAGIC %sql
# MAGIC select constructor_id, count(*) from f1_processed.constructors 
# MAGIC group by constructor_id
# MAGIC order by constructor_id

# COMMAND ----------

#constructor_final_df.write.parquet(f'{processed_folder_path}/constructors', mode='overwrite')

# COMMAND ----------

#df = spark.read.parquet(f'{processed_folder_path}/constructors')
#display(df)

# COMMAND ----------

dbutils.notebook.exit("Success")