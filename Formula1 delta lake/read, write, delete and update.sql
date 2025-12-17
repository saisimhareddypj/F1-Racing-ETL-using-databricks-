-- Databricks notebook source
create database if not exists demo
location '/mnt/f1projectdata/demo'

-- COMMAND ----------

-- MAGIC %run "../Formula1 configuration/includes"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df = spark.read.option("inferSchema", True).json(f"{raw_folder_path}/2021-03-28/results.json")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df.write.format("delta").mode("overwrite").save("/mnt/f1projectdata/demo/extresults")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("demo.partresults")
-- MAGIC

-- COMMAND ----------

select * from demo.results


-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df.write.format("delta").mode("overwrite").saveAsTable("demo.manresults")

-- COMMAND ----------

describe extended demo.manresults

-- COMMAND ----------

show partitions demo.partresults

-- COMMAND ----------

create table demo.extresults
using delta
location "/mnt/f1projectdata/demo/extresults"

-- COMMAND ----------

select * from demo.extresults

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_ext_df = spark.read.format("delta").load("/mnt/f1projectdata/demo/extresults")
-- MAGIC        
-- MAGIC display(results_ext_df)

-- COMMAND ----------

UPDATE demo.results
set points = 11 - position
where position <= 10


-- COMMAND ----------

DELETE from demo.results
where position > 10

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from delta.tables import DeltaTable
-- MAGIC df = DeltaTable.forPath(spark, "dbfs:/user/hive/warehouse/demo.db/results")  # Ensure this path points to a Delta table or convert it if necessary.
-- MAGIC df.update("position <= 10", {"points": "21-position"})

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from delta.tables import DeltaTable
-- MAGIC df = DeltaTable.forPath(spark, "dbfs:/user/hive/warehouse/demo.db/results")  # Ensure this path points to a Delta table or convert it if necessary.
-- MAGIC df.delete("points = 0")

-- COMMAND ----------

select * from demo.results