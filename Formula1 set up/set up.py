# Databricks notebook source
# MAGIC %md
# MAGIC ### Accessing ADLS using service principal
# MAGIC

# COMMAND ----------

application_id = "f2adfb79-ed40-4cfc-9e25-d1fb90eff4f1" 

tenant_id = "fb917bd9-718a-4a9e-8220-f9f362c7fe76"

secret_value = "qSb8Q~YQxKimDgRG5vPZt8vftLGuy42Hn.z2CccG"

# COMMAND ----------

secretkeyvalue = dbutils.secrets.get(scope='formula1scope', key='secval')
applicationidvalue = dbutils.secrets.get(scope='formula1scope', key='app-val')
tenantidvalue = dbutils.secrets.get(scope='formula1scope', key='ten-val')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.f1projectdata.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.f1projectdata.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.f1projectdata.dfs.core.windows.net", applicationidvalue)
spark.conf.set("fs.azure.account.oauth2.client.secret.f1projectdata.dfs.core.windows.net", secretkeyvalue)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.f1projectdata.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenantidvalue}/oauth2/v2.0/token")

# COMMAND ----------

dbutils.fs.ls("abfss://demo@f1projectdata.dfs.core.windows.net/") 

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope='formula1scope')