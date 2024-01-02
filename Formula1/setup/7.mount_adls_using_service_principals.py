# Databricks notebook source
# MAGIC %md
# MAGIC ###Mount Azure Data Lake using Service Principal
# MAGIC ##Steps to follows:
# MAGIC
# MAGIC  - get client_id; tenantID, and client_secret from the vault;
# MAGIC  - set spark configuration with App./Client ID, Directory/TenantID & Secret
# MAGIC  - call the file utility mount to mount the storage
# MAGIC  - explore other file system utilities
# MAGIC  

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

client_id = dbutils.secrets.get(scope="formula1-secret-scope", key="formula1-clientID")
tenant_id = dbutils.secrets.get(scope="formula1-secret-scope", key="formulaTenantID")
client_secret_value = dbutils.secrets.get(scope="formula1-secret-scope", key="clientSecretValue")

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret_value,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    }


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@sonyadatalakestorage.dfs.core.windows.net/",
  mount_point = "/mnt/sonyadatalakestorage/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/sonyadatalakestorage/demo"))


# COMMAND ----------

display(spark.read.csv("/mnt/sonyadatalakestorage/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.unmount('/mnt/sonyadatalakestorage/demo'))

# COMMAND ----------


