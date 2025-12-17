-- Databricks notebook source
-- MAGIC %run "../Formula1 configuration/includes"

-- COMMAND ----------

create database if not exists f1_raw;

-- COMMAND ----------

use f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CSV Files

-- COMMAND ----------

drop table if exists f1_raw.circuits;
create table if not exists f1_raw.circuits(
    circuitId Integer,
    circuitRef String,
    name String,
    location String,
    country String,
    lat Double,
    lng Double,
    alt Integer,
    url String
)
using csv
options(path "/mnt/f1projectdata/raw/circuits.csv", header true)

                                   

-- COMMAND ----------

drop table if exists f1_raw.races;
create table if not exists f1_raw.races(
    raceId Integer,
    year Integer,
    round Integer,
    circuitId Integer,
    name String,
    date Date,
    time String,
    url String
)
using csv
options(path "/mnt/f1projectdata/raw/races.csv", header true)


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## JSON Files

-- COMMAND ----------

drop table if exists f1_raw.constructors;
create table if not exists f1_raw.constructors(
    constructorId Integer,
    constructorRef String,
    name String,
    nationality String,
    url String
)
using json
options(path "/mnt/f1projectdata/raw/constructors.json")

-- COMMAND ----------

drop table if exists f1_raw.drivers;
create table if not exists f1_raw.drivers(
    driverId Integer,
    driverRef String,
    number Integer,
    code String,
    name struct<forename:String, surname:String>,
    dob Date,
    nationality String,
    url String
)
using json
options(path "/mnt/f1projectdata/raw/drivers.json")

-- COMMAND ----------

drop table if exists f1_raw.results;
create table if not exists f1_raw.results(
    resultId Integer,
    raceId Integer,
    driverId Integer,
    constructorId Integer,
    number Integer,
    position Integer,
    positionText String,
    positionOrder Integer,
    points Float,
    laps Integer,
    time String,
    milliseconds Integer,
    fastestLap Integer,
    rank Integer,
    fastestLapTime String,
    fastestLapSpeed Float
)
using json
options(path "/mnt/f1projectdata/raw/results.json")

-- COMMAND ----------

drop table if exists f1_raw.pitstops;
create table if not exists f1_raw.pitstops(
    driverId Integer,
    duration String,
    lap Integer,
    milliseconds Integer,
    raceId Integer,
    stop Integer,
    time string
)
using json
options(path "/mnt/f1projectdata/raw/pit_stops.json", multiline true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Folder files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Folder CSV

-- COMMAND ----------

drop table if exists f1_raw.laptimes;
create table if not exists f1_raw.laptimes(
    raceId Integer,
    driverId Integer,
    lap Integer,
    position Integer,
    time String,
    milliseconds Integer
)
using csv
options (path='/mnt/f1projectdata/raw/lap_times', header true)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Folder JSON

-- COMMAND ----------

drop table if exists f1_raw.qualifying;
create table if not exists f1_raw.qualifying(
    constructorId Integer,
    driverId Integer,
    number Integer,
    position Integer,
    q1 String,
    q2 String,
    q3 String,
    qualifyingId Integer,
    raceId Integer
)
using json
options(path "/mnt/f1projectdata/raw/qualifying", multiline true)