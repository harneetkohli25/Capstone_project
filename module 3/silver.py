# Databricks notebook source
display(dbutils.fs.ls("dbfs:/pipelines/11f3fb9c-5faa-4330-a561-482fe8b7240c/tables/"))


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt


# COMMAND ----------

# MAGIC %run "../module 2/bronze_store"

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
#     schema = StructType([
#     StructField("valid_from", DateType(), True),
# ])
    
    hospital_df = dlt.read('hospital_raw')

    hospital_df = hospital_df.fillna(0)

    hospital_df = hospital_df.withColumn("valid_from", to_date(col("valid_from"), "dd-MM-yyyy"))
    hospital_df = hospital_df.withColumn("valid_upto", to_date(col("valid_upto"), "dd-MM-yyyy"))
    
    # Convert 'valid_from' and 'valid_upto' columns to the desired format
    hospital_df = hospital_df.withColumn("valid_from", date_format(col("valid_from"), "dd/MM/yyyy"))
    hospital_df = hospital_df.withColumn("valid_upto", date_format(col("valid_upto"), "dd/MM/yyyy"))

    # hospital_df = hospital_df.withColumn("valid_from", col("valid_from").cast("string"))

    # Write the cleaned DataFrame as a Delta Live table
    hospital_df.write.format("delta").mode("append").option("mergeSchema", "true").partitionBy("sector").saveAsTable("hospital_clean")

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

# @dlt.expect_all({"valid_phone":"len(patient_Phone) == 10"})

def patients_clean():
    patients_df = dlt.read('patients_raw')

    patients_df = patients_df.withColumn("dob", to_date(col("dob"), "dd-MM-yyyy"))
    
    # Convert 'dob' columns to the desired format
    patients_df = patients_df.withColumn("dob", date_format(col("dob"), "dd/MM/yyyy"))

    # Write the cleaned DataFrame as a Delta Live table
    patients_df.write.format("delta").mode("append").option("mergeSchema", "true").partitionBy("dob").saveAsTable("patients_clean")

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
    policies_df = dlt.read('policies_raw')

    policies_df = policies_df.withColumn("policy_start_date", to_date(col("policy_start_date"), "MM/dd/yyyy")) \
                             .withColumn("policy_end_date", to_date(col("policy_end_date"), "MM/dd/yyyy"))

    policies_df = policies_df.withColumn("policy_start_date", date_format(col("policy_start_date"), "dd/MM/yyyy")) \
                             .withColumn("policy_end_date", date_format(col("policy_end_date"), "dd/MM/yyyy"))
    
    policies_df.write.format("delta").mode("append").option("mergeSchema", "true").partitionBy("policy_start_date").saveAsTable("policies_clean")

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
    plans_df =  dlt.read('plans_raw')


    # Write the cleaned DataFrame as a Delta Live table
    plans_df.write.format("delta").mode("append").option("mergeSchema", "true").partitionBy("payment_type").saveAsTable("plans_clean")

    return plans_df

# COMMAND ----------



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
    new_admissions_partition_df = dlt.read('new_admissions_partition_raw')


    df = new_admissions_partition_df.withColumn(
    "D_O_A",
    when(new_admissions_partition_df["D_O_A"].contains("-"), date_format(to_date(new_admissions_partition_df["D_O_A"], "dd-MM-yyyy"), "dd/MM/yyyy"))
    .when(new_admissions_partition_df["D_O_A"].contains("/"), date_format(to_date(new_admissions_partition_df["D_O_A"], "M/dd/yyyy"), "dd/MM/yyyy"))
    .otherwise(to_date(new_admissions_partition_df["D_O_A"], "dd/MM/yyyy"))
   
)

    df.write.format("delta").mode("append").option("mergeSchema", "true").partitionBy("outcome").saveAsTable("new_admissions_partition_clean")

    return df


# COMMAND ----------

@dlt.create_table(
  comment="Clean the patient history data",
  table_properties={
    "wecure.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)
def patient_history_clean():
    patient_history_df = dlt.read('patient_history_raw')

    for column in patient_history_df.columns:
        patient_history_df = patient_history_df.withColumn(column, when(col(column).isNull(), "0").otherwise(col(column)))

    # Write the cleaned DataFrame as a Delta Live table
    patient_history_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("patient_history_clean")

    return patient_history_df

# COMMAND ----------


@dlt.create_table(
  comment="This table is clean partitioned by outcome",
  partition_cols=["outcome"],
  table_properties={
    "wecure.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)


def new_admissions_partition_clean_2():
    new_admissions_partition_2_df = dlt.read('admission_new_raw_2')


    df2 = new_admissions_partition_2_df.withColumn(
    "D_O_A",
    when(new_admissions_partition_2_df["D_O_A"].contains("-"), date_format(to_date(new_admissions_partition_2_df["D_O_A"], "dd-MM-yyyy"), "dd/MM/yyyy"))
    .when(new_admissions_partition_2_df["D_O_A"].contains("/"), date_format(to_date(new_admissions_partition_2_df["D_O_A"], "M/dd/yyyy"), "dd/MM/yyyy"))
    .otherwise(to_date(new_admissions_partition_2_df["D_O_A"], "dd/MM/yyyy"))
   
)

    df2.write.format("delta").mode("append").option("mergeSchema", "true").partitionBy("outcome").saveAsTable("new_admissions_partition_clean_2")

    return df2


# COMMAND ----------


@dlt.create_table(
  comment="This table is clean partitioned by outcome",
  partition_cols=["outcome"],
  table_properties={
    "wecure.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)


def new_admissions_partition_clean_3():
    new_admissions_partition_3_df = dlt.read('admission_new_raw_3')


    df3 = new_admissions_partition_3_df.withColumn(
    "D_O_A",
    when(new_admissions_partition_3_df["D_O_A"].contains("-"), date_format(to_date(new_admissions_partition_3_df["D_O_A"], "dd-MM-yyyy"), "dd/MM/yyyy"))
    .when(new_admissions_partition_3_df["D_O_A"].contains("/"), date_format(to_date(new_admissions_partition_3_df["D_O_A"], "M/dd/yyyy"), "dd/MM/yyyy"))
    .otherwise(to_date(new_admissions_partition_3_df["D_O_A"], "dd/MM/yyyy"))
   
)

    df3.write.format("delta").mode("append").option("mergeSchema", "true").partitionBy("outcome").saveAsTable("new_admissions_partition_clean_3")

    return df3


# COMMAND ----------


@dlt.create_table(
  comment="This table is clean partitioned by outcome",
  partition_cols=["outcome"],
  table_properties={
    "wecure.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)


def new_admissions_partition_clean_4():
    new_admissions_partition_4_df = dlt.read('admission_new_raw_4')


    df4 = new_admissions_partition_4_df.withColumn(
    "D_O_A",
    when(new_admissions_partition_4_df["D_O_A"].contains("-"), date_format(to_date(new_admissions_partition_4_df["D_O_A"], "dd-MM-yyyy"), "dd/MM/yyyy"))
    .when(new_admissions_partition_4_df["D_O_A"].contains("/"), date_format(to_date(new_admissions_partition_4_df["D_O_A"], "M/dd/yyyy"), "dd/MM/yyyy"))
    .otherwise(to_date(new_admissions_partition_4_df["D_O_A"], "dd/MM/yyyy"))
   
)

    df4.write.format("delta").mode("append").option("mergeSchema", "true").partitionBy("outcome").saveAsTable("new_admissions_partition_clean_4")

    return df4


# COMMAND ----------


@dlt.create_table(
  comment="This table is clean partitioned by outcome",
  partition_cols=["outcome"],
  table_properties={
    "wecure.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)


def new_admissions_partition_clean_5():
    new_admissions_partition_5_df = dlt.read('admission_new_raw_5')


    df5 = new_admissions_partition_5_df.withColumn(
    "D_O_A",
    when(new_admissions_partition_5_df["D_O_A"].contains("-"), date_format(to_date(new_admissions_partition_5_df["D_O_A"], "dd-MM-yyyy"), "dd/MM/yyyy"))
    .when(new_admissions_partition_5_df["D_O_A"].contains("/"), date_format(to_date(new_admissions_partition_5_df["D_O_A"], "M/dd/yyyy"), "dd/MM/yyyy"))
    .otherwise(to_date(new_admissions_partition_5_df["D_O_A"], "dd/MM/yyyy"))
   
)

    df5.write.format("delta").mode("append").option("mergeSchema", "true").partitionBy("outcome").saveAsTable("new_admissions_partition_clean_5")

    return df5


# COMMAND ----------


@dlt.create_table(
  comment="This table is clean partitioned by outcome",
  partition_cols=["outcome"],
  table_properties={
    "wecure.quality": "silver",
    "pipelines.autoOptimize.managed": "true"
  }
)


def new_admissions_partition_clean_6():
    new_admissions_partition_6_df = dlt.read('admission_new_raw_6')


    df6 = new_admissions_partition_6_df.withColumn(
    "D_O_A",
    when(new_admissions_partition_6_df["D_O_A"].contains("-"), date_format(to_date(new_admissions_partition_6_df["D_O_A"], "dd-MM-yyyy"), "dd/MM/yyyy"))
    .when(new_admissions_partition_6_df["D_O_A"].contains("/"), date_format(to_date(new_admissions_partition_6_df["D_O_A"], "M/dd/yyyy"), "dd/MM/yyyy"))
    .otherwise(to_date(new_admissions_partition_6_df["D_O_A"], "dd/MM/yyyy"))
   
)

    df6.write.format("delta").mode("append").option("mergeSchema", "true").partitionBy("outcome").saveAsTable("new_admissions_partition_clean_6")

    return df6


# COMMAND ----------


