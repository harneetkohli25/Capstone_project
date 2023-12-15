# Databricks notebook source
hospital_df = spark.read.format("delta").table("hive_metastore.default.hospital_clean")
new_admissions_partition_df = spark.read.format("delta").table("hive_metastore.default.new_admissions_partition_clean")
patient_history_df = spark.read.format("delta").table("hive_metastore.default.patient_history_clean")
patients_df = spark.read.format("delta").table("hive_metastore.default.patients_clean")
plans_df = spark.read.format("delta").table("hive_metastore.default.plans_clean")
policies_df = spark.read.format("delta").table("hive_metastore.default.policies_clean")

# COMMAND ----------

hospital_df.createOrReplaceTempView("hospital")
new_admissions_partition_df.createOrReplaceTempView("new_admissions_partition")
patient_history_df.createOrReplaceTempView("patient_history")
patients_df.createOrReplaceTempView("patients")
plans_df.createOrReplaceTempView("plans")
policies_df.createOrReplaceTempView("policies")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from policies

# COMMAND ----------


