# Databricks notebook source
# MAGIC %md
# MAGIC ###Access Azure Data Lake using a service principal
# MAGIC
# MAGIC  - register the Azure AD application / Service principal;
# MAGIC  - generate a secret/password;
# MAGIC  - set the spark configuration with app/client id, directory/tenant id and secret;
# MAGIC  - assign the role "storage blob data contributor" to the Data Lake.
# MAGIC  

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

client_id = dbutils.secrets.get(scope="formula1-secret-scope", key="formula1-clientID")
tenant_id = dbutils.secrets.get(scope="formula1-secret-scope", key="formulaTenantID")
client_secret_value = dbutils.secrets.get(scope="formula1-secret-scope", key="clientSecretValue")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.sonyadatalakestorage.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.sonyadatalakestorage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.sonyadatalakestorage.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.sonyadatalakestorage.dfs.core.windows.net", client_secret_value)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.sonyadatalakestorage.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@sonyadatalakestorage.dfs.core.windows.net"))


# COMMAND ----------

display(spark.read.csv("abfss://demo@sonyadatalakestorage.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


