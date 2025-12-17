-- Databricks notebook source
-- MAGIC %run "../Formula1 configuration/includes"

-- COMMAND ----------

use demo

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("delta").saveAsTable("demo.race_results_py")

-- COMMAND ----------

use demo;
show tables

-- COMMAND ----------

describe extended demo.race_results_py

-- COMMAND ----------

use demo

-- COMMAND ----------

create or replace table race_results_sql
as
select * from demo.race_results_py
where race_year = 2020


-- COMMAND ----------

describe extended race_results_sql

-- COMMAND ----------

drop table if exists race_results_sql
