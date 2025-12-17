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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import concat
from pyspark.sql.functions import to_timestamp

# COMMAND ----------

races_schema = StructType(fields = [
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)]
)

# COMMAND ----------

races_df =spark.read.csv(f'{raw_folder_path}/{v_file_date}/races.csv', header = True, schema = races_schema)

# COMMAND ----------

races_renamed_df = races_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("year", "race_year") \
.withColumnRenamed("circuitId", "circuit_id")

# COMMAND ----------

races_added_column_df = races_renamed_df.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"))
races_final_df = add_injestion_date(races_added_column_df)
races_final_df = races_final_df.withColumn("datasource", lit(data_source)).withColumn("file_date", lit(v_file_date))


# COMMAND ----------

races_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

#races_final_df.write.parquet(f'{processed_folder_path}/races', mode = "overwrite", partitionBy="race_year")

# COMMAND ----------

#df = spark.read.parquet(f'{processed_folder_path}/races')

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table if exists f1_processed.races

# COMMAND ----------

#overwrite_partition(races_final_df, 'f1_processed', 'races', 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(*) from f1_processed.races
# MAGIC group by race_id
# MAGIC order by race_id

# COMMAND ----------

#race_results1_df = spark.read.parquet(f"{raw_folder_path}/2021-03-21/race_results")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

#race_results1_df = spark.read.parquet(f"{raw_folder_path}/2021-03-21/race_results")