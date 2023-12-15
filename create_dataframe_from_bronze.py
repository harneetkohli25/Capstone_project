# Databricks notebook source
# MAGIC %fs unmount /mnt/wecure

# COMMAND ----------

# MAGIC %run "./mount data in bronze"

# COMMAND ----------

path = "dbfs:/mnt/wecure/hospitals.csv"
hospital_df = spark.read.csv(path,header=True,inferSchema=True)

# COMMAND ----------

path1 = "dbfs:/mnt/wecure/new_admissions_partition_1.csv"
new_admissions_partition_df = spark.read.csv(path1,header=True,inferSchema=True)

# COMMAND ----------

path2 = "dbfs:/mnt/wecure/patients.csv"
patients_df = spark.read.csv(path2,header=True,inferSchema=True)

# COMMAND ----------

path3 = "dbfs:/mnt/wecure/plans.csv"
plans_df = spark.read.csv(path3,header=True,inferSchema=True)

# COMMAND ----------

path4 = "dbfs:/mnt/wecure/policies_new.csv"
policies_df = spark.read.csv(path4,header=True,inferSchema=True)

# COMMAND ----------

path5 = "dbfs:/mnt/wecure/patient_history.json"
patient_history_df =  spark.read.format("json").option("multiLine","true").load(path5)

# COMMAND ----------

display(hospital_df)

# COMMAND ----------

display(new_admissions_partition_df)

# COMMAND ----------

display(patients_df)

# COMMAND ----------

display(plans_df)

# COMMAND ----------

display(policies_df)

# COMMAND ----------

display(patient_history_df)
