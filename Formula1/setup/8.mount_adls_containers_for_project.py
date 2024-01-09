# Databricks notebook source
# MAGIC %md
# MAGIC ###Mount Azure Data Lake Containers for the project
# MAGIC
# MAGIC  

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    
    #get the secrets from the Key Vault
    client_id = dbutils.secrets.get(scope="formula1-secret-scope", key="formula1-clientID")
    tenant_id = dbutils.secrets.get(scope="formula1-secret-scope", key="formulaTenantID")
    client_secret_value = dbutils.secrets.get(scope="formula1-secret-scope", key="clientSecretValue")


    #set the configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
               "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
               "fs.azure.account.oauth2.client.id": client_id,
               "fs.azure.account.oauth2.client.secret": client_secret_value,
               "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

    #unmount the point if already mounted:
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    #mount the storage container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)


    display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Row container
# MAGIC mount_adls

# COMMAND ----------

mount_adls('sonyadatalakestorage', 'raw')

# COMMAND ----------

mount_adls('sonyadatalakestorage', 'presentation')

# COMMAND ----------

mount_adls('sonyadatalakestorage', 'processed')

# COMMAND ----------

mount_adls('sonyadatalakestorage', 'dldemo')

# COMMAND ----------

dbutils.fs.ls('mnt/sonyadatalakestorage/dldemo')
