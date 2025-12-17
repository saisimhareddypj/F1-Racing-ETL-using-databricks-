-- Databricks notebook source
create database if not exists f1_demo

-- COMMAND ----------

show databases

-- COMMAND ----------

describe f1_demo

-- COMMAND ----------

select current_database()

-- COMMAND ----------

SHOW TABLES in f1_demo

-- COMMAND ----------

use f1_demo

-- COMMAND ----------

select current_database()