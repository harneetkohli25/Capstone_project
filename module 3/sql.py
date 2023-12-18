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
# MAGIC select * from patients

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from patient_history

# COMMAND ----------

# MAGIC %md Q1

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT DISTINCT p.patient_id, p.Full_Name
# MAGIC FROM patients p
# MAGIC JOIN policies po ON p.patient_id = po.patient_id
# MAGIC WHERE po.daily_premium > (SELECT AVG(daily_premium) FROM policies);

# COMMAND ----------

# MAGIC %md Q2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC         p.plan_id,
# MAGIC         ROUND(COALESCE(AVG(CASE WHEN pl.inpatient_care THEN p.insurance_coverage END), 0), 2) AS inpatient_avg_coverage,
# MAGIC         ROUND(COALESCE(AVG(CASE WHEN pl.outpatient_care THEN p.insurance_coverage END), 0), 2) AS outpatient_avg_coverage,
# MAGIC         ROUND(COALESCE(AVG(CASE WHEN pl.inpatient_care THEN p.daily_premium END), 0), 2) AS inpatient_avg_premium,
# MAGIC         ROUND(COALESCE(AVG(CASE WHEN pl.outpatient_care THEN p.daily_premium END), 0), 2) AS outpatient_avg_premium
# MAGIC     FROM
# MAGIC         policies p
# MAGIC     JOIN
# MAGIC         plans pl
# MAGIC     ON
# MAGIC         p.plan_id = pl.plan_id
# MAGIC     GROUP BY
# MAGIC         p.plan_id

# COMMAND ----------

# MAGIC %md Q3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     h.hosp_id as Branch_id,
# MAGIC     h.name AS hospital_name,
# MAGIC     COUNT(DISTINCT CASE WHEN ph.DM = 'yes' THEN ph.patient_id END) AS DM_count,
# MAGIC     COUNT(DISTINCT CASE WHEN ph.HTN = 'yes' THEN ph.patient_id END) AS HTN_count,
# MAGIC     COUNT(DISTINCT CASE WHEN ph.DM = 'yes' AND ph.HTN = 'yes' THEN ph.patient_id END) AS Both,
# MAGIC     COUNT(DISTINCT CASE WHEN ph.DM = '0' AND ph.HTN = '0' THEN ph.patient_id END) AS Neither
# MAGIC FROM
# MAGIC     hospital h
# MAGIC JOIN
# MAGIC     new_admissions_partition nap ON h.hosp_id = nap.hospital_id
# MAGIC JOIN
# MAGIC     patient_history ph ON nap.patient_id = ph.patient_id
# MAGIC GROUP BY
# MAGIC     h.hosp_id,h.name;

# COMMAND ----------

# MAGIC %md Q4

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     h.hosp_id as branch_id,
# MAGIC     h.name AS hospital_name,
# MAGIC     AVG(YEAR(CURRENT_DATE()) - RIGHT(dob, 4)) AS avg_patient_age,
# MAGIC     COUNT(DISTINCT nap.admission_id) AS total_admissions,
# MAGIC     CASE
# MAGIC         WHEN AVG(YEAR(CURRENT_DATE()) - RIGHT(dob, 4)) >= 50 OR COUNT(DISTINCT nap.admission_id) >= 1000 THEN 'Tier 1'
# MAGIC         WHEN AVG(YEAR(CURRENT_DATE()) - RIGHT(dob, 4)) BETWEEN 40 AND 49 OR COUNT(DISTINCT nap.admission_id) BETWEEN 500 AND 999 THEN 'Tier 2'
# MAGIC         ELSE 'Tier 3'
# MAGIC     END AS branch_tier
# MAGIC FROM
# MAGIC     hospital h
# MAGIC JOIN
# MAGIC     new_admissions_partition nap ON h.hosp_id = nap.hospital_id
# MAGIC JOIN
# MAGIC     patients p ON nap.patient_id = p.patient_id
# MAGIC GROUP BY
# MAGIC     h.hosp_id, h.name;
# MAGIC

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


