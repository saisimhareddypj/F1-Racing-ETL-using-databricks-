-- Databricks notebook source
-- MAGIC %run "../Formula1 configuration/includes"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC drivers_day1 = spark.read\
-- MAGIC .option("inferSchema", True)\
-- MAGIC .json(f"{raw_folder_path}/2021-03-28/drivers.json")\
-- MAGIC .filter("driverId between 1 and 10")\
-- MAGIC .select("driverId", "dob", "name.forename", "name.surname")

-- COMMAND ----------

use demo

-- COMMAND ----------

-- MAGIC %python
-- MAGIC drivers_day1.createOrReplaceTempView("drivers_day1")
-- MAGIC drivers_day2.createOrReplaceTempView("drivers_day2")
-- MAGIC drivers_day3.createOrReplaceTempView("drivers_day3")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import upper

-- COMMAND ----------

-- MAGIC %python
-- MAGIC drivers_day2 = spark.read\
-- MAGIC .option("inferSchema", True)\
-- MAGIC .json(f"{raw_folder_path}/2021-03-28/drivers.json")\
-- MAGIC .filter("driverId between 6 and 15")\
-- MAGIC .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))
-- MAGIC        
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC drivers_day3 = spark.read\
-- MAGIC .option("inferSchema", True)\
-- MAGIC .json(f"{raw_folder_path}/2021-03-28/drivers.json")\
-- MAGIC .filter("driverId between 1 and 5 or driverId between 16 and 20")\
-- MAGIC .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

-- COMMAND ----------

create table if not exists demo.driversmerge(
  driverId long,
  dob string,
  forename string,
  surname string,
  created_date date,
  updated_date date
)
using delta

-- COMMAND ----------

merge into demo.driversmerge tgt
using drivers_day1 sou
on tgt.driverId = sou.driverId
when matched then 
update set tgt.dob = sou.dob,
tgt.forename = sou.forename,
tgt.surname = sou.surname,
tgt.updated_date = current_timestamp()
when not matched then
insert(driverId, dob, forename, surname, created_date)
values(sou.driverId, sou.dob, sou.forename, sou.surname, current_timestamp())


-- COMMAND ----------

merge into demo.driversmerge tgt
using drivers_day2 sou
on tgt.driverId = sou.driverId
when matched then 
update set tgt.dob = sou.dob,
tgt.forename = sou.forename,
tgt.surname = sou.surname,
tgt.updated_date = current_timestamp()
when not matched then
insert(driverId, dob, forename, surname, created_date)
values(sou.driverId, sou.dob, sou.forename, sou.surname, current_timestamp())

-- COMMAND ----------

merge into demo.driversmerge tgt
using drivers_day3 sou
on tgt.driverId = sou.driverId
when matched then 
update set tgt.dob = sou.dob,
tgt.forename = sou.forename,
tgt.surname = sou.surname,
tgt.updated_date = current_timestamp()
when not matched then
insert(driverId, dob, forename, surname, created_date)
values(sou.driverId, sou.dob, sou.forename, sou.surname, current_timestamp())

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from pyspark.sql.functions import current_timestamp, upper
-- MAGIC        
-- MAGIC
-- MAGIC from delta.tables import DeltaTable
-- MAGIC df= DeltaTable.forPath(spark, "dbfs:/user/hive/warehouse/demo.db/driversmerge")
-- MAGIC
-- MAGIC df.alias("tgt")\
-- MAGIC   .merge(
-- MAGIC     drivers_day3.alias("src"),
-- MAGIC     "tgt.driverId = src.driverId")\
-- MAGIC   .whenMatchedUpdate(
-- MAGIC     set = {
-- MAGIC       "dob": "src.dob", 
-- MAGIC       "forename":"src.forename", 
-- MAGIC       "surname": "src.surname", 
-- MAGIC       "updated_date": "current_timestamp"
-- MAGIC       })\
-- MAGIC   .whenNotMatchedInsert(
-- MAGIC     values = {
-- MAGIC     "driverId": "src.driverId",
-- MAGIC     "dob": "src.dob",
-- MAGIC     "forename": "src.forename",
-- MAGIC     "surname": "src.surname",
-- MAGIC     "created_date": "current_timestamp"
-- MAGIC   })\
-- MAGIC     .execute()

-- COMMAND ----------

select * from demo.driversmerge 


-- COMMAND ----------

describe history demo.driversmerge

-- COMMAND ----------

select * from demo.driversmerge version as of 1

-- COMMAND ----------

select * from demo.driversmerge TIMESTAMP AS OF '2025-12-03T05:10:57.000+00:00'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC final_df = spark.read.format('delta').option("timestampAsof", "2025-12-03T05:10:57.000+00:00").load('dbfs:/user/hive/warehouse/demo.db/driversmerge')
-- MAGIC display(final_df)

-- COMMAND ----------

select * from demo.driversmerge version as of 7

-- COMMAND ----------

 SET spark.databricks.delta.retentionDurationCheck.enabled = false;
 VACUUM demo.driversmerge retain 120 hours