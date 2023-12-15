# Databricks notebook source
container_name = "wecure001"
account_name = "wecure"
storage_account_key = "4VhbFBbmpqYpBMhkhpNA8L/9ptfb3BS+WaHYOzBkhQu0Ngf7gM3W+XlbxpeFWa5V19kBOLNihqtr+ASt3RlM6g=="
dbutils.fs.mount(
    source="wasbs://{0}@{1}.blob.core.windows.net".format(container_name, account_name),
    mount_point="/mnt/wecure",
    extra_configs={"fs.azure.account.key.{0}.blob.core.windows.net".format(account_name): storage_account_key}
)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/wecure"))

# COMMAND ----------


