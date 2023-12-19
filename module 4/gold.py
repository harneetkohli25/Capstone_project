# Databricks notebook source
# MAGIC %run "../module 3/silver"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt
from pyspark.sql import functions as F

# COMMAND ----------

def calculate_hospital_stats(admissions_df, hospitals_df):
    
    # Join DataFrames
    merged_spark_df = admissions_df.join(
        hospitals_df,
        admissions_df.hospital_id == hospitals_df.hosp_id,
        'inner'
    )

    # Calculate total admissions per hospital
    total_admissions_per_hospital = merged_spark_df.groupBy('name').agg(F.count('admission_id').alias('Total_Admissions'))

    # Calculate average duration of stay per hospital
    average_duration_per_hospital = merged_spark_df.groupBy('name').agg(F.mean('duration_of_stay').alias('Average_Duration_of_Stay'))

    # Calculate total ICU admissions per hospital
    total_icu_admissions_per_hospital = merged_spark_df.groupBy('name').agg(F.count('duration_of_intensive_unit_stay').alias('Total_ICU_Admissions'))

    # Join results into a single DataFrame
    hospital_stats_df = total_admissions_per_hospital.join(
        average_duration_per_hospital,
        'name'
    ).join(
        total_icu_admissions_per_hospital,
        'name'
    )

    return hospital_stats_df

# COMMAND ----------

def calculate_plan_utilization(admissions_df, plans_df, policies_df, patients_df):
    # Alias for each DataFrame to resolve ambiguity
    admissions_alias = admissions_df.alias("admissions")
    plans_alias = plans_df.alias("plans")
    policies_alias = policies_df.alias("policies")
    patients_alias = patients_df.alias("patients")

    # Join admissions with patients to get the policy_number
    admissions_df = admissions_alias.join(
        patients_alias,
        admissions_alias.patient_id == patients_alias.patient_id,
        'inner'
    )

    # Join admissions with policies to get the plan_id
    admissions_df = admissions_df.join(
        policies_alias,
        patients_alias.patient_id == policies_alias.patient_id,
        'inner'
    )

    # Join the DataFrames on plan_id
    merged_spark_df = admissions_df.join(
        plans_alias,
        admissions_df.plan_id == plans_alias.plan_id,
        'inner'
    )

    # Number of admissions per plan
    admissions_per_plan = merged_spark_df.groupBy('plans.plan_id').agg(F.count('admissions.admission_id').alias('Number_of_Admissions'))

    # Average duration of stay per plan
    avg_duration_per_plan = merged_spark_df.groupBy('plans.plan_id').agg(F.mean('admissions.duration_of_stay').alias('Average_Duration_of_Stay'))

    # Calculate total admissions
    total_admissions = merged_spark_df.count()

    # Calculate Percentage of admissions with outpatient care coverage
    outpatient_coverage_percentage = 0.0
    if total_admissions != 0:
        outpatient_coverage_percentage = (merged_spark_df.filter(merged_spark_df.outpatient_care == 1).count() / total_admissions) * 100

    # Create a summary DataFrame
    plan_utilization_df = admissions_per_plan.join(
        avg_duration_per_plan,
        'plan_id'
    ).withColumn(
        'Percentage_of_Outpatient_Coverage',
        F.lit(outpatient_coverage_percentage)
    )

    return plan_utilization_df

# COMMAND ----------

def calculate_geographical_analysis(admissions_df, hospitals_df):
    

    # Merge the DataFrames on hospital_id
    merged_spark_df = admissions_df.join(
        hospitals_df,
        admissions_df.hospital_id == hospitals_df.hosp_id,
        'inner'
    )

    # Number of admissions per state
    admissions_per_state = merged_spark_df.groupBy('state').agg(F.count('admission_id').alias('Number_of_Admissions'))

    # Average duration of stay per state
    avg_duration_per_state = merged_spark_df.groupBy('state').agg(F.mean('duration_of_stay').alias('Average_Duration_of_Stay'))

    # Create a summary DataFrame
    geographical_analysis_df = admissions_per_state.join(
        avg_duration_per_state,
        'state'
    )

    return geographical_analysis_df

# COMMAND ----------

@dlt.create_table(
    comment="Aggregated hospital stats",
    table_properties={
        "wecure.quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def hospital_stats_agg():
    
    # Read Delta tables
    patients_df = dlt.read("patients_clean")
    policies_df = dlt.read("policies_clean")
    plans_df = dlt.read("plans_clean")
    hospital_df = dlt.read("hospital_clean")
    new_admissions_partition_df = dlt.read("new_admissions_partition_clean")

    # Call the previously defined functions to perform aggregations
    hospital_stats = calculate_hospital_stats(new_admissions_partition_df, hospital_df)

    

    return hospital_stats

# COMMAND ----------

@dlt.create_table(
    comment="Aggregated plan utilization",
    table_properties={
        "wecure.quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def plan_utilization_agg():
    
    # Read Delta tables
    patients_df = dlt.read("patients_clean")
    policies_df = dlt.read("policies_clean")
    plans_df = dlt.read("plans_clean")
    hospital_df = dlt.read("hospital_clean")
    new_admissions_partition_df = dlt.read("new_admissions_partition_clean")

    # Call the previously defined functions to perform aggregations
    plan_utilization = calculate_plan_utilization(new_admissions_partition_df, plans_df, policies_df, patients_df)

    
    return plan_utilization

# COMMAND ----------

@dlt.create_table(
    comment="Aggregated geographical analysis",
    table_properties={
        "wecure.quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def geographical_analysis_agg():
    
    # Read Delta tables
    patients_df = dlt.read("patients_clean")
    policies_df = dlt.read("policies_clean")
    plans_df = dlt.read("plans_clean")
    hospital_df = dlt.read("hospital_clean")
    new_admissions_partition_df = dlt.read("new_admissions_partition_clean")

    # Call the previously defined functions to perform aggregations
    geographical_analysis = calculate_geographical_analysis(new_admissions_partition_df,hospital_df)

    
    return geographical_analysis

# COMMAND ----------


