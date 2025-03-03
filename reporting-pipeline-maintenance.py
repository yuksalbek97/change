# Databricks notebook source
##########################-NOTES-########################################

# Date                 Created By               Reason
# 11/09/2022           Prashant Rai             US331059 (Reporting Pipeline Maintenance Script)

###################### Add comments below ###############################

# COMMAND ----------

from delta.tables import *

# COMMAND ----------
env_tag_lst = spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags")

env_tag = ""

json_object = json.loads(env_tag_lst)
for i in json_object:
    if list(i.values())[0] == 'environmenttag':
        env_tag = list(i.values())[1].upper()

print(env_tag)
dfm_reporting_path = f"abfss://bronze@sapbmuscafpdatalake{env_tag.lower()}.dfs.core.windows.net/afp/dfm/reporting/"

dbutils.widgets.text("uc_run","True")
if dbutils.widgets.get("uc_run")=='True':
  if env_tag=="PROD":
    catalog_value="puma-prod"
  if env_tag=="QA":
    catalog_value="puma_qa"
  if env_tag=="DEV":
    catalog_value="puma-dev"
  if env_tag=="STG":
    catalog_value="puma-stg"
else if dbutils.widgets.get("uc_run")=='False':
  catalog_value="hive_metastore"
spark.sql(f"USE CATALOG `{catalog_value}`")

# COMMAND ----------

# MAGIC %md ###Adhoc

# COMMAND ----------

over_ride = 1 #set this value to 1 if merge is needed in PROD else keep it 0
query0 = "Drop Table If Exists reporting.uwdfm_t_process_time"
spark.sql(query0)

if DeltaTable.isDeltaTable(spark, dfm_reporting_path + "uwdfm_t_process_time"):
  query1 = "DELETE FROM reporting.uwdfm_t_process_time"
  spark.sql(query1)
  dbutils.fs.rm(dfm_reporting_path + "uwdfm_t_process_time/", True)
    
query2 = "Drop Table If Exists reporting.uwdfm_t_generic_pipeline_rpt"
spark.sql(query2)

if DeltaTable.isDeltaTable(spark, dfm_reporting_path + "uwdfm_t_generic_pipeline_rpt"):
  query3 = "DELETE FROM reporting.uwdfm_t_generic_pipeline_rpt`"
  spark.sql(query3)
  dbutils.fs.rm(dfm_reporting_path + "uwdfm_t_generic_pipeline_rpt/", True)

# COMMAND ----------

# MAGIC %run ./reporting-current-default-pipeline $VarA=over_ride
