# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

display(dbutils.fs.ls("/mnt/wecure001/dataset/unzipped_data"))

# COMMAND ----------

@dlt.create_table(
  comment="The Raw patient history table",
  table_properties={
    "wecure.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def patient_history_raw():
    patient_history_df =  spark.read.format("json").option("multiLine","true").load("dbfs:/mnt/wecure001/dataset/unzipped_data/patient_history.json",inferSchema=True)
    patient_history_df = patient_history_df.withColumnRenamed("ATYPICAL CHEST PAIN", "ATYPICAL_CHEST_PAIN") \
    .withColumnRenamed("CARDIOGENIC SHOCK", "CARDIOGENIC_SHOCK") \
    .withColumnRenamed("CHEST INFECTION", "CHEST_INFECTION") \
    .withColumnRenamed("CVA BLEED", "CVA_BLEED") \
    .withColumnRenamed("CVA INFRACT", "CVA_INFRACT") \
    .withColumnRenamed("HEART FAILURE", "HEART_FAILURE") \
    .withColumnRenamed("INFECTIVE ENDOCARDITIS", "INFECTIVE_ENDOCARDITIS") \
    .withColumnRenamed("NEURO CARDIOGENIC SYNCOPE", "NEURO_CARDIOGENIC_SYNCOPE") \
    .withColumnRenamed("RAISED CARDIAC ENZYMES", "RAISED_CARDIAC_ENZYMES") \
    .withColumnRenamed("PRIOR CMP", "PRIOR_CMP") \
    .withColumnRenamed("PULMONARY EMBOLISM", "PULMONARY_EMBOLISM") \
    .withColumnRenamed("SEVERE ANAEMIA", "SEVERE_ANAEMIA") \
    .withColumnRenamed("STABLE ANGINA", "STABLE_ANGINA") \
    .withColumnRenamed("SMOKING ", "SMOKING") 
     
    return patient_history_df

# COMMAND ----------

@dlt.create_table(
  comment="The Raw hospital Table",
  table_properties={
    "wecure.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def hospital_raw():
    hospital_df = spark.read.csv("dbfs:/mnt/wecure001/dataset/unzipped_data/hospitals.csv", header=True, inferSchema=True)
    return hospital_df

# COMMAND ----------

@dlt.create_table(
  comment="The Raw new admissions partition Table",
  table_properties={
    "wecure.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def new_admissions_partition_raw():
    new_admissions_partition_df = spark.read.csv("dbfs:/mnt/wecure001/dataset/unzipped_data/new_admissions_partition_1.csv", header=True, inferSchema=True)
    new_admissions_partition_df = new_admissions_partition_df.withColumnRenamed("D.O.A", "D_O_A") \
    .withColumnRenamed("TYPE OF ADMISSION-EMERGENCY/OPD", "type_of_admission") \
    .withColumnRenamed("DURATION OF STAY", "duration_of_stay") \
    .withColumnRenamed("OUTCOME", "outcome") \
    .withColumnRenamed("duration of intensive unit stay", "duration_of_intensive_unit_stay") 
    return new_admissions_partition_df

# COMMAND ----------

@dlt.create_table(
  comment="The Raw patients Table",
  table_properties={
    "wecure.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def patients_raw():
    patients_df = spark.read.csv("dbfs:/mnt/wecure001/dataset/unzipped_data/patients.csv", header=True, inferSchema=True)
    return patients_df

# COMMAND ----------

@dlt.create_table(
  comment="The Raw plans Table",
  table_properties={
    "wecure.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def plans_raw():
    plans_df = spark.read.csv("dbfs:/mnt/wecure001/dataset/unzipped_data/plans.csv", header=True, inferSchema=True)
    return plans_df

# COMMAND ----------

@dlt.create_table(
  comment="The Raw policies Table",
  table_properties={
    "wecure.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def policies_raw():
    policies_df = spark.read.csv("dbfs:/mnt/wecure001/dataset/unzipped_data/policies_new.csv", header=True, inferSchema=True)
    return policies_df

# COMMAND ----------

display(dbutils.fs.ls("/mnt/wecure001/dataset/incrementunzipped_data"))

# COMMAND ----------

@dlt.create_table(
  comment="The Raw incremnetal admission Table",
  table_properties={
    "wecure.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def admission_new_raw_2():
    admission_2_df = spark.read.csv("dbfs:/mnt/wecure001/dataset/incrementunzipped_data/new_admissions_partition_2.csv", header=True, inferSchema=True)
    admission_2_df = admission_2_df.withColumnRenamed("D.O.A", "D_O_A") \
    .withColumnRenamed("TYPE OF ADMISSION-EMERGENCY/OPD", "type_of_admission") \
    .withColumnRenamed("DURATION OF STAY", "duration_of_stay") \
    .withColumnRenamed("OUTCOME", "outcome") \
    .withColumnRenamed("duration of intensive unit stay", "duration_of_intensive_unit_stay")
    return admission_2_df

# COMMAND ----------

@dlt.create_table(
  comment="The Raw incremnetal admission Table",
  table_properties={
    "wecure.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def admission_new_raw_3():
    admission_3_df = spark.read.csv("dbfs:/mnt/wecure001/dataset/incrementunzipped_data/new_admissions_partition_3.csv", header=True, inferSchema=True)
    admission_3_df = admission_3_df.withColumnRenamed("D.O.A", "D_O_A") \
    .withColumnRenamed("TYPE OF ADMISSION-EMERGENCY/OPD", "type_of_admission") \
    .withColumnRenamed("DURATION OF STAY", "duration_of_stay") \
    .withColumnRenamed("OUTCOME", "outcome") \
    .withColumnRenamed("duration of intensive unit stay", "duration_of_intensive_unit_stay")
    return admission_3_df

# COMMAND ----------

@dlt.create_table(
  comment="The Raw incremnetal admission Table",
  table_properties={
    "wecure.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def admission_new_raw_4():
    admission_4_df = spark.read.csv("dbfs:/mnt/wecure001/dataset/incrementunzipped_data/new_admissions_partition_4.csv", header=True, inferSchema=True)
    admission_4_df = admission_4_df.withColumnRenamed("D.O.A", "D_O_A") \
    .withColumnRenamed("TYPE OF ADMISSION-EMERGENCY/OPD", "type_of_admission") \
    .withColumnRenamed("DURATION OF STAY", "duration_of_stay") \
    .withColumnRenamed("OUTCOME", "outcome") \
    .withColumnRenamed("duration of intensive unit stay", "duration_of_intensive_unit_stay")
    return admission_4_df

# COMMAND ----------

@dlt.create_table(
  comment="The Raw incremnetal admission Table",
  table_properties={
    "wecure.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def admission_new_raw_5():
    admission_5_df = spark.read.csv("dbfs:/mnt/wecure001/dataset/incrementunzipped_data/new_admissions_partition_5.csv", header=True, inferSchema=True)
    admission_5_df = admission_5_df.withColumnRenamed("D.O.A", "D_O_A") \
    .withColumnRenamed("TYPE OF ADMISSION-EMERGENCY/OPD", "type_of_admission") \
    .withColumnRenamed("DURATION OF STAY", "duration_of_stay") \
    .withColumnRenamed("OUTCOME", "outcome") \
    .withColumnRenamed("duration of intensive unit stay", "duration_of_intensive_unit_stay")
    return admission_5_df

# COMMAND ----------

@dlt.create_table(
  comment="The Raw incremnetal admission Table",
  table_properties={
    "wecure.quality": "bronze",
    "pipelines.autoOptimize.managed": "true"
  }
)
def admission_new_raw_6():
    admission_6_df = spark.read.csv("dbfs:/mnt/wecure001/dataset/incrementunzipped_data/new_admissions_partition_6.csv", header=True, inferSchema=True)
    admission_6_df = admission_6_df.withColumnRenamed("D.O.A", "D_O_A") \
    .withColumnRenamed("TYPE OF ADMISSION-EMERGENCY/OPD", "type_of_admission") \
    .withColumnRenamed("DURATION OF STAY", "duration_of_stay") \
    .withColumnRenamed("OUTCOME", "outcome") \
    .withColumnRenamed("duration of intensive unit stay", "duration_of_intensive_unit_stay")
    return admission_6_df

# COMMAND ----------


