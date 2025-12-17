# Databricks notebook source
def mount_adls(storage_account_name, container_name):
    # Get secrets from Key Vault
    secretkeyvalue = dbutils.secrets.get(scope='formula1scope', key='secval')
    applicationidvalue = dbutils.secrets.get(scope='formula1scope', key='app-val')
    tenantidvalue = dbutils.secrets.get(scope='formula1scope', key='ten-val')
    
    # Set spark configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": applicationidvalue,
          "fs.azure.account.oauth2.client.secret": secretkeyvalue,          
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenantidvalue}/oauth2/token"}
    
    
    # Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    # Mount the storage account container
    dbutils.fs.mount(
      source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = configs)
    
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls('f1projectdata', 'raw')

# COMMAND ----------

mount_adls('f1projectdata', 'processed')

# COMMAND ----------

mount_adls('f1projectdata', 'presentation')