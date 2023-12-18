# Databricks notebook source
container_name = "wecure001"
account_name = "wecuredata"

# dbutils.fs.mount(
# source = "wasbs://{0}@{1}.blob.core.windows.net".format(container_name, account_name),
# mount_point = "/mnt/wecure001",
#  extra_configs = {"fs.azure.account.key.{0}.blob.core.windows.net".format(account_name): storage_account_key}
#   )

# COMMAND ----------

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import requests
from io import BytesIO

# COMMAND ----------

import subprocess
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

zip_file_url = "https://mentorskool-platform-uploads.s3.ap-south-1.amazonaws.com/documents/0a1227ae-adea-4a5f-903f-60b946ed7352_83d04ac6-cb74-4a96-a06a-e0d5442aa126_HealthCare_data.zip"

local_zip_file_path = "/dbfs/mnt/wecure001/dataset/data.zip"
local_unzip_dir = "/dbfs/mnt/wecure001/dataset/unzipped_data"

subprocess.run(["wget", "-O", local_zip_file_path, zip_file_url], check=True)
subprocess.run(["unzip", "-q", local_zip_file_path, "-d", local_unzip_dir], check=True)

azure_connection_string = "DefaultEndpointsProtocol=https;AccountName=capstonp;AccountKey=+NbWQyuYaB/+tu53Ghq5qlrpqff+pL0Jdtrj7nD7KO99Vn/0Ro5wnjIBAVrxPzfuKH6MUq1IAxoo+AStm/3b9A==;EndpointSuffix=core.windows.net"

container_name = "wecure001"
folder_name = "unzipped"

blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
container_client = blob_service_client.get_container_client(container_name)

local_files = os.listdir(local_unzip_dir)

for file_name in local_files:
    local_file_path = os.path.join(local_unzip_dir, file_name)
    blob_path = os.path.join(folder_name, file_name)
    blob_client = container_client.get_blob_client(blob_path)
    with open(local_file_path, "rb") as dataset:
        blob_client.upload_blob(dataset)

print("Uploaded all files to Azure Blob Storage in the 'unzipped' folder")

if len(local_files) == len(container_client.list_blobs(name_starts_with=folder_name)):
    print("All files have been uploaded. Stopping the execution.")
    os.remove(local_zip_file_path)
    for file_name in local_files:
        local_file_path = os.path.join(local_unzip_dir, file_name)
        os.remove(local_file_path)
    print("Cleaned up local files")
    exit()

print("Not all files have been uploaded. The script will continue.")

