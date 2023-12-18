# Databricks notebook source
display(dbutils.fs.ls("dbfs:/pipelines/11f3fb9c-5faa-4330-a561-482fe8b7240c/tables/"))


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

@dlt.create_table(
  comment="Cleaned hospital data partitioned by sector",
  partition_cols=["sector"],
  table_properties={
    "wecure.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
def hospital_clean():
    hospital_df = spark.read.format("delta").load("dbfs:/pipelines/11f3fb9c-5faa-4330-a561-482fe8b7240c/tables/hospital_raw/")

    hospital_df = hospital_df.fillna(0)

    hospital_df = hospital_df.withColumn("valid_from", to_date(col("valid_from"), "dd-MM-yyyy"))
    hospital_df = hospital_df.withColumn("valid_upto", to_date(col("valid_upto"), "dd-MM-yyyy"))
    
    # Convert 'valid_from' and 'valid_upto' columns to the desired format
    hospital_df = hospital_df.withColumn("valid_from", date_format(col("valid_from"), "dd/MM/yyyy"))
    hospital_df = hospital_df.withColumn("valid_upto", date_format(col("valid_upto"), "dd/MM/yyyy"))

    # Write the cleaned DataFrame as a Delta Live table
    hospital_df.write.format("delta").mode("append").partitionBy("sector").saveAsTable("hospital_clean")

    return hospital_df

# COMMAND ----------

@dlt.create_table(
  comment="Cleaned patients data partitioned by dob",
  partition_cols=["dob"],
  table_properties={
    "wecure.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)

@dlt.expect_all({"valid_phone":"len(patient_Phone) == 10"})

def patients_clean():
    patients_df = spark.read.format("delta").load("dbfs:/pipelines/11f3fb9c-5faa-4330-a561-482fe8b7240c/tables/patients_raw/")

    patients_df = patients_df.withColumn("dob", to_date(col("dob"), "dd-MM-yyyy"))
    
    # Convert 'dob' columns to the desired format
    patients_df = patients_df.withColumn("dob", date_format(col("dob"), "dd/MM/yyyy"))

    # Write the cleaned DataFrame as a Delta Live table
    patients_df.write.format("delta").mode("append").partitionBy("dob").saveAsTable("patients_clean")

    return patients_df

# COMMAND ----------

@dlt.create_table(
  comment="Cleaned policies data partitioned by policy_start_date",
  partition_cols=["policy_start_date"],
  table_properties={
    "wecure.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
def policies_clean():
    policies_df = spark.read.format("delta").load("dbfs:/pipelines/11f3fb9c-5faa-4330-a561-482fe8b7240c/tables/policies_raw/")

    policies_df = policies_df.withColumn("policy_start_date", to_date(col("policy_start_date"), "MM/dd/yyyy")) \
                             .withColumn("policy_end_date", to_date(col("policy_end_date"), "MM/dd/yyyy"))

    policies_df = policies_df.withColumn("policy_start_date", date_format(col("policy_start_date"), "dd/MM/yyyy")) \
                             .withColumn("policy_end_date", date_format(col("policy_end_date"), "dd/MM/yyyy"))
    
    policies_df.write.format("delta").mode("append").partitionBy("policy_start_date").saveAsTable("policies_clean")

    return policies_df

# COMMAND ----------

@dlt.create_table(
  comment="This table is clean plans data partitioned by payment_type",
  partition_cols=["payment_type"],
  table_properties={
    "wecure.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
def plans_clean():
    plans_df = spark.read.format("delta").load("dbfs:/pipelines/11f3fb9c-5faa-4330-a561-482fe8b7240c/tables/plans_raw/")


    # Write the cleaned DataFrame as a Delta Live table
    plans_df.write.format("delta").mode("append").partitionBy("payment_type").saveAsTable("plans_clean")

    return plans_df

# COMMAND ----------

@dlt.create_table(
  comment="This table is clean partitioned by outcome",
  partition_cols=["outcome"],
  table_properties={
    "wecure.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
def new_admissions_partition_clean():
    new_admissions_partition_df = spark.read.format("delta").load("dbfs:/pipelines/11f3fb9c-5faa-4330-a561-482fe8b7240c/tables/new_admissions_partition_raw/")


    # Write the cleaned DataFrame as a Delta Live table
    new_admissions_partition_df.write.format("delta").mode("append").partitionBy("outcome").saveAsTable("new_admissions_partition_clean")

    return new_admissions_partition_df

# COMMAND ----------

@dlt.create_table(
  comment="Clean the patient history data",
  table_properties={
    "wecure.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
def patient_history_clean():
    patient_history_df = spark.read.format("delta").load("dbfs:/pipelines/11f3fb9c-5faa-4330-a561-482fe8b7240c/tables/patient_history_raw/")

    for column in patient_history_df.columns:
        patient_history_df = patient_history_df.withColumn(column, when(col(column).isNull(), "0").otherwise(col(column)))

    # Write the cleaned DataFrame as a Delta Live table
    patient_history_df.write.format("delta").mode("append").saveAsTable("patient_history_clean")

    return patient_history_df

# COMMAND ----------


