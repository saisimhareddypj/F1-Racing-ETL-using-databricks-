# Databricks notebook source
dbutils.notebook.run("/Workspace/Formula1/Formula1 data injestion/circuits csv data injestion", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

dbutils.notebook.run("/Workspace/Formula1/Formula1 data injestion/constructors json data injestion", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

dbutils.notebook.run("/Workspace/Formula1/Formula1 data injestion/drivers json data injestion", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

dbutils.notebook.run("/Workspace/Formula1/Formula1 data injestion/lap times folder data injestion", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

dbutils.notebook.run("/Workspace/Formula1/Formula1 data injestion/pit stops json data injestion", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

dbutils.notebook.run("/Workspace/Formula1/Formula1 data injestion/qualifying folder data injestion", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

dbutils.notebook.run("/Workspace/Formula1/Formula1 data injestion/races csv data injestion", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

dbutils.notebook.run("/Workspace/Formula1/Formula1 data injestion/results json data injestion", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})