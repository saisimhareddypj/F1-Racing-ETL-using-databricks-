-- Databricks notebook source
-- MAGIC %run "../Formula1 configuration/includes"

-- COMMAND ----------

select current_database()

-- COMMAND ----------

create database if not exists demo

-- COMMAND ----------

use demo

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("delta").option("path", f"{presentation_folder_path}/demo.race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

describe extended race_results_ext_py

-- COMMAND ----------

create table race_results_ext_sql
(
race_year	int,
race_name	string,
race_date	date,
race_location	string,
driver_name	string,
driver_number	int,
driver_nationality	string,
team	string,
grid	int,
fastest_lap	int,
points	float,
race_time	string,
position	int,
created_date	timestamp
)
using parquet
location '/mnt/f1projectdata/presentation/race_results_ext_sql'

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

insert into demo.race_results_ext_sql 
select * from race_results_ext_py where race_year = 2020

-- COMMAND ----------

select count(*) from demo.race_results_ext_sql

-- COMMAND ----------

show tables in demo


-- COMMAND ----------

drop table hive_metastore.demo.race_results_ext_sql