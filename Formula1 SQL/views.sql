-- Databricks notebook source
-- MAGIC %run "../Formula1 configuration/includes"

-- COMMAND ----------

create or replace temp view v_race_results 
as 
select * from demo.race_results_py 
where race_year = 2019 

-- COMMAND ----------

create or replace global temp view gv_race_results 
as 
select * from demo.race_results_py 
where race_year = 2019 

-- COMMAND ----------

show tables in global_temp

-- COMMAND ----------

create or replace view demo.v_race_results 
as 
select * from demo.race_results_py 
where race_year = 2019 

-- COMMAND ----------

show tables in demo