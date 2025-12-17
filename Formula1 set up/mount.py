# Databricks notebook source
secretkeyvalue = dbutils.secrets.get(scope='formula1scope', key='secval')
applicationidvalue = dbutils.secrets.get(scope='formula1scope', key='app-val')
tenantidvalue = dbutils.secrets.get(scope='formula1scope', key='ten-val')

# COMMAND ----------

dbutils.secrets.list(scope='formula1scope')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": applicationidvalue,
          "fs.azure.account.oauth2.client.secret": secretkeyvalue,          
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenantidvalue}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@f1projectdata.dfs.core.windows.net/",
  mount_point = "/mnt/f1projectdata/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/f1projectdata/demo"))

# COMMAND ----------


display(spark.read.csv("/mnt/f1projectdata/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())