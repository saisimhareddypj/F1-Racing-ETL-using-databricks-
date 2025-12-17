-- Databricks notebook source
use database f1_processed

-- COMMAND ----------

select * from drivers

-- COMMAND ----------

select name, nationality, dob, 
rank() over(partition by nationality order by dob desc) as age_order
from drivers