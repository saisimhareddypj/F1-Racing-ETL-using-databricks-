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

from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DateType
)

# COMMAND ----------

name_schema = StructType([
    StructField("forename", StringType(), nullable=True),
    StructField("surname", StringType(), nullable=True)
])

# COMMAND ----------

drivers_schema = StructType([
    StructField("driverId", IntegerType(), nullable=False),
    StructField("driverRef", StringType(), nullable=False),
    StructField("number", IntegerType(), nullable=True),
    StructField("code", StringType(), nullable=True),
    StructField("name", name_schema),
    StructField("dob", DateType(), nullable=True),
    StructField("nationality", StringType(), nullable=True),
    StructField("url", StringType(), nullable=False)
])


# COMMAND ----------

drivers_df = spark.read.json(f'{raw_folder_path}/{v_file_date}/drivers.json', schema = drivers_schema)

# COMMAND ----------

drivers_renamed_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("driverRef", "driver_ref")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit, concat

# COMMAND ----------

drivers_added_df =add_injestion_date(drivers_renamed_df)

# COMMAND ----------

drivers_concat_df = drivers_added_df.withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))

# COMMAND ----------

display(drivers_concat_df)

# COMMAND ----------

drivers_selected_df = drivers_concat_df.select(col("driver_id"), col("driver_ref"), col("number"), col("code"), col("name"), col("nationality"), col("dob"), col("ingestion_date"))

# COMMAND ----------

drivers_final_df = drivers_selected_df.withColumn("datasource", lit(data_source))\
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

#display(drivers_selected_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table if exists f1_processed.drivers

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

#overwrite_partition(drivers_final_df, 'f1_processed', 'drivers', 'driver_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select driver_id, count(*) from f1_processed.drivers
# MAGIC group by driver_id
# MAGIC order by driver_id

# COMMAND ----------

#drivers_selected_df.write.parquet(f'{processed_folder_path}/drivers', mode="overwrite")

# COMMAND ----------

#df = spark.read.parquet(f'{processed_folder_path}/drivers')

# COMMAND ----------

dbutils.notebook.exit("Success")