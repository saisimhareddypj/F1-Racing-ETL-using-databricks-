-- Databricks notebook source
create database if not exists f1_processed
location "/mnt/f1projectdata/processed"

-- COMMAND ----------

describe database f1_processed