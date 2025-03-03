# Databricks notebook source
##########################-NOTES-########################################

# Date                 Created By               Reason
# 06/06/2022           Prashant Rai             US231293 (Reporting Pipeline)

###################### Add comments below ###############################
# 06/16/2022           Prashant Rai             US231586 (Added COGS module)
# 06/16/2022           Prashant Rai             US243261 (Added Input Param module)
# 07/07/2022           Prashant Rai             US256690 (Updated COGS Logic)
# 07/12/2022           Prashant Rai             US259903
# 07/20/2022           Prashant Rai             US261165 (Formulary Shift Strategy)
# 09/02/2022           Prashant Rai             US280021 (Convert process time & input param to delta)
# 09/09/2022.          Prashant Rai             US282444 (Remove unnecessary columns from COGS to improve performance)
# 09/16/2022           Prashant Rai             US281228 (Add Date dimension table)
# 10/28/2022           Prashant Rai             US312718 (Fix Process Time Table)
# 11/02/2022           Prashant Rai             DE101442 (Fix Table Sync Issues)
# 11/11/2022           Prashant Rai             US331059 (Reporting Pipeline Maintenance logic)
# 01/11/2023           Prashant Rai             Changed input param source to dfm table to avoid sync issues
# 01/19/2023           Prashant Rai             DE112995
# 01/27/2023           Prashant Rai             US206033 - LDD updates for COGS
# 02/15/2023           Prashant Rai             DE116685 - Handle Obsolete control tables
# 03/02/2023           Prashant Rai             Deactivated all modules except COGS
# 06/26/2023           Prashant Rai             TA436499- Fix DFM storage naming issue
# 07/03/2023           Prashant Rai             US406074- Pipeline sync error notification
# 10/05/2023           Prashant Rai             US459045- Add GPI Drug Mix Module
# 10/17/2023           Prashant Rai             US474646- Modify GPI logic
# 11/14/2023           Prashant Rai             US472111- Updated COGS Module to source from DFM output table
# 11/27/2023           Prashant Rai             US472114- Added COGS Model module & updated cogs to include new fields
# 01/02/2024           Prashant Rai             US506827- Added/Updated fields to regular & model based COGS
# 01/10/2024           Prashant Rai             US474623- Added GPI Model based module
# 01/17/2024           Prashant Rai             US509232- Hotfix and proposed formulary name changes to model tables
# 01/24/2024           Prashant Rai             US509232- LDD Hotfix to model tables
# 04/11/2024           Prashant Rai             US547721- LDD & NW changes to COGS & COGS Model
# 04/11/2024           Prashant Rai             US547263- LDD & NW changes to GPI & GPI Model
# 04/16/2024           Prashant Rai             US547751- Avg Processing time fix for delta capture
# 04/30/2024           Prashant Rai             US556949- COGS/GPI Hotfix(model)
# 05/15/2024           Prashant Rai             US553816- GPI Hotfix for Drug Name
# 06/28/2024           Prashant Rai             US581806- Identify & eliminate IRF experimental DR’s from reporting
# 10/04/2024           Prashant Rai             US587410- DFMO schema changes for COGS & GPI
#########################################################################

# COMMAND ----------

spark.conf.set("spark.databricks.io.cache.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "1800")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled","true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled","true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.databricks.delta.checkLatestSchemaOnRead","false")

# COMMAND ----------

from pyspark.sql.functions import lit,col,when,current_timestamp,countDistinct,count,avg
from pyspark.sql.functions import sum as sparkSum
from pyspark.sql.types import *
from pyspark.sql import types
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from delta.tables import *
from pyspark.sql.functions import *
from typing import List
#from dbutils import FileInfo
from dbruntime.dbutils import FileInfo
from pathlib import Path
import os
import re
from datetime import datetime
from pyspark.sql.functions import explode, sequence, to_date
import sys
import json
sys.setrecursionlimit(10000)


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS reporting;

# COMMAND ----------

def read_output(artifact):
  df = spark.read.format("delta").load(dfm_output_path + artifact + "/")
  return df


# COMMAND ----------

# MAGIC %md ###Pipeline Parameters

# COMMAND ----------

##### Determine Environment(Non-Prod or PROD) #####
env_lst = spark.conf.getAll()
envColumns = ["env_attribute","env_value"]
env_df = spark.createDataFrame(data=env_lst, schema = envColumns)
env_df=env_df.filter((env_df.env_value.like ('%{"key":"environmentsubtype","value":"Dev"}%')) | (env_df.env_value.like ('%{"key":"environmentsubtype","value":"QA"}%')) | (env_df.env_value.like ('%{"key":"environmentsubtype","value":"Staging"}%')))

if env_df.count()!=0:
  env1 = 1   #Non-Prod
else:
  env1 = 0   #Prod

print(env1)

# COMMAND ----------

##### Determine sub environment #####
env_tag_lst = spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags")

env_tag = ""

json_object = json.loads(env_tag_lst)
for i in json_object:
    if list(i.values())[0] == 'environmenttag':
        env_tag = list(i.values())[1].upper()

print(env_tag)
dfm_output_path = f"abfss://bronze@sapbmuscafpdatalake{env_tag.lower()}.dfs.core.windows.net/afp/dfm/module_output/"
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

# This column will update environment variable only if triggerd from maintenance script and allows merge in PROD environment
if env1 == 0:
  try:
    if over_ride==1:
      env1 = over_ride
  except Exception as e:
    env1 = 0

# COMMAND ----------

# List all tables present in schema #######
tblList = [table.tableName for table in spark.sql("SHOW TABLES in reporting").collect()]

# COMMAND ----------

#tblList

# COMMAND ----------

# Caching
dfm_activity=spark.sql("""select parent_req_id, request_id, task_id, status from dfm.uwdfm_t_activity_process_control""").cache()
dfm_param=spark.sql("""select parent_request_id, request_id, task_id from dfm.uwdfm_t_req_input_param""").cache()

# COMMAND ----------

# MAGIC %md ###Delta Logic

# COMMAND ----------

#### This cell finds the requests for which the associated modules have to be refreshed/created

######Capture persisted state of processed requests if the table already exists(needed to find new requests)##############################
if "uwdfm_t_process_time" in tblList:
  process_time_initial_df= spark.sql("""
  Select 
  uw_req_id,req_gid,cpt_id
  from reporting.uwdfm_t_process_time
  """)
  process_time_initial_df.createOrReplaceTempView("process_time_initial")

#######Capture current state of processed requests(needed to find new requests)##############################
fullDF = spark.sql("""
  select client_description, parent_req_id, req_id, task_id, 
  create_date,
  modified_date,
  inp_load_ts,
  status,
  '0' as elapsed_time
  from reporting.dfm_uwdfm_t_activity_process_control_rpt_hist
union all
select
b.client_description, dapc.parent_req_id, dapc.request_id, dapc.task_id,
date_format(dapc.start_dttm,"yyyy-MM-dd hh:mm:ss") as create_date,
date_format(dapc.end_dttm,"yyyy-MM-dd hh:mm:ss") as modified_date,
date_format(coalesce(b.inp_load_ts,'9999-12-31 00:00:00') ,"yyyy-MM-dd hh:mm:ss") as inp_load_ts,
status,
dapc.elapsed_time
from (
  select parent_req_id,
  --request_id,
  explode((split(request_id, '[_]'))) as request_id,
  task_id,
  start_dttm,
  end_dttm,
  status,
  elapsed_time
  from
  dfm.uwdfm_t_activity_process_control
 ) dapc
join dfm.uwdfm_t_req_input_param as b on dapc.parent_req_id = b.parent_request_id
  and dapc.request_id = b.request_id 
  and dapc.task_id = b.task_id 
where 1=1
and dapc.status="COMPLETE" 
and dapc.task_id like 'UW%'
and coalesce(b.data_request_type,'DFM') ='DFM'
""")

import datetime
 
def elapsedTime(tmStart=None, tmStop=None):
  
  t1 = datetime.datetime.strptime(tmStart,"%Y-%m-%d %H:%M:%S")
  t2 = datetime.datetime.strptime(tmStop,"%Y-%m-%d %H:%M:%S")
  diff = t2 - t1
  
  days, seconds = diff.days, diff.seconds
   
  return "%s-%s"%(days,seconds)

#-- Create User Function
elapsedTimeUDF = udf(lambda x, y: elapsedTime(x,y), StringType())
 
#-- Add processing time column
#fullDFv0 = fullDF.withColumn('funcData', (elapsedTimeUDF(fullDF['modified_date'],fullDF['submitted_time'])))
fullDFv0 = fullDF.withColumn('funcData', (elapsedTimeUDF(fullDF['inp_load_ts'],fullDF['modified_date'])))
 
#-- Append days and seconds elapsed
fullDFv1 = fullDFv0.withColumn('funcSec', split(fullDFv0['funcData'], '-').getItem(1))\
  .withColumn('prcs_days', split(fullDFv0['funcData'], '-').getItem(0))
 
#-- Append minutes,total seconds, hours and time elapsed
fullDFv2 = fullDFv1\
  .withColumn('prcs_mins', round(((fullDFv1['funcSec'] % 3600)/60),0).cast("int"))\
  .withColumn('prcs_sec', round((fullDFv1['funcSec'] % 60),0).cast("int"))\
  .withColumn('prcs_tot_sec', (fullDFv1['prcs_days'] * 24 * 60 * 60 + fullDFv1['funcSec']))\
  .withColumn('prcs_hours', round(((fullDFv1['prcs_days'] * 24) + (fullDFv1['funcSec']/3600)),0).cast("int"))
 
fullDFv3 = fullDFv2.withColumn('prcs_time', concat_ws(":",round(fullDFv2['prcs_hours'],0),round(fullDFv2['prcs_mins'],0),round(fullDFv2['prcs_sec'],0)))
fullDFv3.createOrReplaceTempView("processTime2")

fullDFv4=spark.sql("""
select 
 client_description
 ,parent_req_id as uw_req_id
 ,req_id as req_gid
 ,task_id as cpt_id
 ,create_date
 ,modified_date
 ,inp_load_ts
 ,funcData
 ,funcSec
 ,prcs_days
 ,prcs_mins
 ,prcs_sec
 ,case when prcs_tot_sec is null then 2.5*60*60 
       when inp_load_ts >='2023-01-14 00:00:00' then split_part(split_part(elapsed_time, '.', 1),':',1)*60*60+split_part(split_part(elapsed_time, '.', 1),':',2)*60+split_part(split_part(elapsed_time, '.', 1),':',3)
 else prcs_tot_sec end as prcs_tot_sec
 ,prcs_hours
 ,prcs_time
 ,status
from processTime2 pt2
""")
fullDFv4.createOrReplaceTempView("processTime3")

######If process time table exists then Capture requests that were created between the current run and last run else capture all the requests###########################
if "uwdfm_t_process_time" in tblList:
  process_time_delta_df=spark.sql("""
  Select 
  uw_req_id,req_gid,cpt_id
  from processTime3
  where 1=1
  and concat(uw_req_id,req_gid,cpt_id) NOT IN
  (Select
  concat(uw_req_id,req_gid,cpt_id)
  from process_time_initial)
  and client_description is not null
  """)
else:
  process_time_delta_df=spark.sql("""
  Select 
  uw_req_id,req_gid,cpt_id--,client_description
  from processTime3
  where client_description is not null
  """)
  process_time_initial_df= spark.sql("""
  Select 
  *
  from processTime3
  where client_description is not null
  """)
  process_time_initial_df.createOrReplaceTempView("process_time_initial")

# COMMAND ----------

# MAGIC %md ###Delta Logic for COGS Model

# COMMAND ----------

if  ("uwdfm_t_cogs_mdl_rpt" in tblList and "uwdfm_t_cogs_rpt" in tblList):
    # print('1')
    process_time_delta_df_cogs_mdl1=spark.sql("""
    Select 
    distinct data_request_id,scenario_id,business_type_id
    from reporting.ce_model_utility_rpt cmu
    join reporting.uwdfm_t_cogs_rpt cr on cr.opportunity_id=cmu.opportunity_id 
    and cr.PARENT_REQUEST_ID=cmu.data_request_id 
    and cr.TASK_ID=cmu.task_id 
    and cr.lob=cmu.business_type 
    and cr.submit_date_cst=cmu.submit_date_cst 
    and cr.Dataset_identifier=cmu.pusedo_brand_generic_definition 
    and cr.PROP_FRMLY_NAME=cmu.formulary
    and cr.FORMULARY_GUID=cmu.formulary_guid
    and cr.NETWORK_GUID=cmu.network_guid
    and upper(cr.type)=upper(cmu.type)
    where 1=1
    and concat(data_request_id,scenario_id,business_type_id) NOT IN
    (Select
    concat(m_data_request_id,m_scenario_id,m_business_type_id)
    from reporting.uwdfm_t_cogs_mdl_rpt)
    """)
elif ("uwdfm_t_cogs_mdl_rpt" in tblList and "uwdfm_t_cogs_rpt" not in tblList):
    # print('2')
    process_time_delta_df_cogs_mdl1=spark.sql("""
    Select 
    distinct data_request_id,scenario_id,business_type_id
    from reporting.ce_model_utility_rpt 
    where 1=1
    and concat(data_request_id,scenario_id,business_type_id) NOT IN
    (Select
    concat(m_data_request_id,m_scenario_id,m_business_type_id)
    from reporting.uwdfm_t_cogs_mdl_rpt)
    """)
else:
    # print('3')
    process_time_delta_df_cogs_mdl1=spark.sql("""
    Select 
    distinct data_request_id,scenario_id,business_type_id
    from reporting.ce_model_utility_rpt
    """)
# process_time_delta_df_cogs_mdl1.createOrReplaceTempView("cogs_mdl_ptd")

# COMMAND ----------

# MAGIC %md ###Delta Logic for GPI Model

# COMMAND ----------

if  ("uwdfm_t_gpi_mdl_rpt" in tblList and "uwdfm_t_gpi_rpt" in tblList):
    # print('1')
    process_time_delta_df_gpi_mdl1=spark.sql("""
    Select 
    distinct data_request_id,scenario_id,business_type_id
    from reporting.ce_model_utility_rpt cmu
    join reporting.uwdfm_t_gpi_rpt cr on cr.opportunity_id=cmu.opportunity_id 
    and cr.PARENT_REQUEST_ID=cmu.data_request_id 
    and cr.TASK_ID=cmu.task_id 
    and cr.lob=cmu.business_type 
    and cr.submit_date_cst=cmu.submit_date_cst 
    and cr.Dataset_identifier=cmu.pusedo_brand_generic_definition 
    and cr.PROP_FRMLY_NAME=cmu.formulary
    --and cr.FORMULARY_GUID=cmu.formulary_guid
    --and cr.NETWORK_GUID=cmu.network_guid
    and upper(cr.type)=upper(cmu.type)
    where 1=1
    and concat(data_request_id,scenario_id,business_type_id) NOT IN
    (Select distinct
    concat(m_data_request_id,m_scenario_id,m_business_type_id)
    from reporting.uwdfm_t_gpi_mdl_rpt)
    """)
elif ("uwdfm_t_gpi_mdl_rpt" in tblList and "uwdfm_t_gpi_rpt" not in tblList):
    # print('2')
    process_time_delta_df_gpi_mdl1=spark.sql("""
    Select 
    distinct data_request_id,scenario_id,business_type_id
    from reporting.ce_model_utility_rpt 
    where 1=1
    and concat(data_request_id,scenario_id,business_type_id) NOT IN
    (Select distinct
    concat(m_data_request_id,m_scenario_id,m_business_type_id)
    from reporting.uwdfm_t_gpi_mdl_rpt)
    """)
else:
    # print('3')
    process_time_delta_df_gpi_mdl1=spark.sql("""
    Select 
    distinct data_request_id,scenario_id,business_type_id
    from reporting.ce_model_utility_rpt
    """)
# process_time_delta_df_cogs_mdl1.createOrReplaceTempView("cogs_mdl_ptd")

# COMMAND ----------

process_time_delta_df.count()

# COMMAND ----------

process_time_delta_df_cogs_mdl1.count()

# COMMAND ----------

process_time_delta_df_gpi_mdl1.count()

# COMMAND ----------

#####If there are no new modules, no new requests & process table is present,then exit the process else continue to refresh #################
# if ("uwdfm_t_cc_topdrugs_rpt" in tblList and "uwdfm_t_cc_state_rpt" in tblList and "uwdfm_t_cc_preshift_rpt" in tblList and "uwdfm_t_cc_summary_rpt" in tblList 
# and "uwdfm_t_cogs_rpt" in tblList and "uwdfm_t_daw9_shifted_rpt" in tblList and "uwdfm_t_formulary_shifted_rpt" in tblList and "uwdfm_t_formulary_shifted_rpt" in tblList and "uwdfm_t_formulary_shifted_rpt_excl" in tblList and "uwdfm_t_generic_pipeline_rpt" in tblList and"uwdfm_t_hg_shifted_rpt" in tblList and "uwdfm_t_pcsk9_shifted_rpt" in tblList and "uwdfm_t_req_input_param_rpt" in tblList and "uwdfm_t_process_time" in tblList and "uwdfm_t_formulary_shift_strategy_rpt" in tblList and "uwdfm_t_dates_dim_rpt" in tblList):
#   if process_time_delta_df.count()==0:
#     dbutils.notebook.exit('STOP-No new records to insert')
    
if ("uwdfm_t_cogs_rpt" in tblList and "uwdfm_t_req_input_param_rpt" in tblList and "uwdfm_t_process_time" in tblList and "uwdfm_t_dates_dim_rpt" in tblList and "uwdfm_t_gpi_rpt" in tblList and "uwdfm_t_cogs_mdl_rpt" in tblList and "uwdfm_t_gpi_mdl_rpt" in tblList):
  if (process_time_delta_df.count()==0 and process_time_delta_df_cogs_mdl1.count()==0) :
    dbutils.notebook.exit('STOP-No new records to insert')    

# COMMAND ----------

# MAGIC %md ###Claims Tagging Reports

# COMMAND ----------

# ### Only execute this cell when the first 'if' condition is satisfied

# if ("uwdfm_t_cc_topdrugs_rpt" not in tblList or "uwdfm_t_cc_state_rpt" not in tblList or "uwdfm_t_cc_preshift_rpt" not in tblList or "uwdfm_t_cc_summary_rpt" not in tblList or process_time_delta_df.count()!=0):

#   claims_preshift_df = (read_output("out_uwdfm_t_cc_claims_preshift")).alias('cpd').join(process_time_delta_df.alias('ptd'),(col('cpd.parent_request_id') ==  col('ptd.uw_req_id'))
#                      & (col('cpd.request_id') ==  col('ptd.req_gid')) & (col('cpd.task_id') ==  col('ptd.cpt_id')),'inner')

#   claims_tag_df = (read_output("out_uwdfm_t_cc_claims_tag")).alias('ctd').join(process_time_delta_df.alias('ptd'),(col('ctd.parent_request_id') ==  col('ptd.uw_req_id'))
#                      & (col('ctd.request_id') ==  col('ptd.req_gid')) & (col('ctd.task_id') ==  col('ptd.cpt_id')),'inner')

#   df_rpt_base = (
#    claims_tag_df.alias('tag')
#   .join(claims_preshift_df.alias('preshift'),col('tag.claimnbr') == col('preshift.claimnbr'), 'left_outer')
#   .select( 
#     col('tag.parent_request_id'),
#     col('tag.request_id').alias("req_gid"),
#     col('tag.task_id'),
#     col('superclient_id'),
#     col('client_id'),
#     col('client_nm'),
#     col('dofii'),
#     col('tag.medd_stus_cd'),
#     col('buyup_class').alias('medd_class'),
#     lit('Y').alias('keepdata'),
#     lit(None).cast('string').alias('pharm38non20'),
#     col('genbrand'),
#     col('model_drug').alias('modeldrug'),
#     col('tag.pmcy').alias('ncpdp'),
#     col('currawp'),
#     col('currwac'),
#     col('ltc_clm'),
#     col('claims'),
#     col('state'),
#     col('tag.claimnbr'),
#     col('lob_biz_type'),
#     col('tag.carrier'),
#     col('tag.account'),
#     col('tag.group'),
#     col('speclock'),
#     col('drugname'),
#     col('srx_drug_flg'),
#     col('awp'),
#     col('lob'),
#     col('tag.dlvry_systm_cd'),
#     col('year').alias("report_year"),
#     col('month').alias("report_month"),
#     col('mbr_gid').alias('memberid'),
#     col('tot_mbr'),
#     col('tag.orgnl_ingrd_cost'),
#     col('tag.clcfee'),
#     col('wac_amt'),
#     col('preshift.dayssply'),
#     col('tag.mc'),
#     when(col("tag.dlvry_systm_cd") != 'M', col("preshift.dayssply")).otherwise(lit(0)).alias('retail_ds'),
#     col('rxnum'),
#     current_timestamp().alias("current_timestamp"))
# )

#   if "uwdfm_t_cc_topdrugs_rpt" not in tblList or "uwdfm_t_cc_state_rpt" not in tblList or "uwdfm_t_cc_preshift_rpt" not in tblList or "uwdfm_t_cc_summary_rpt" not in tblList:
#     process_time_delta_df1=spark.sql("""
#     Select 
#     uw_req_id,req_gid,cpt_id--,client_description
#     from processTime3
#     where client_description is not null
#     """)
#     claims_preshift_df = (read_output("out_uwdfm_t_cc_claims_preshift")).alias('cpd').join(process_time_delta_df1.alias('ptd'),(col('cpd.parent_request_id') ==                           col('ptd.uw_req_id')) & (col('cpd.request_id') ==  col('ptd.req_gid')) & (col('cpd.task_id') ==  col('ptd.cpt_id')),'inner')

#     claims_tag_df = (read_output("out_uwdfm_t_cc_claims_tag")).alias('ctd').join(process_time_delta_df1.alias('ptd'),(col('ctd.parent_request_id') ==  col('ptd.uw_req_id'))
#                      & (col('ctd.request_id') ==  col('ptd.req_gid')) & (col('ctd.task_id') ==  col('ptd.cpt_id')),'inner')

#     df_rpt_base2 = (
#    claims_tag_df.alias('tag')
#   .join(claims_preshift_df.alias('preshift'),col('tag.claimnbr') == col('preshift.claimnbr'), 'left_outer')
#   .select( 
#     col('tag.parent_request_id'),
#     col('tag.request_id').alias("req_gid"),
#     col('tag.task_id'),
#     col('superclient_id'),
#     col('client_id'),
#     col('client_nm'),
#     col('dofii'),
#     col('tag.medd_stus_cd'),
#     col('buyup_class').alias('medd_class'),
#     lit('Y').alias('keepdata'),
#     lit(None).cast('string').alias('pharm38non20'),
#     col('genbrand'),
#     col('model_drug').alias('modeldrug'),
#     col('tag.pmcy').alias('ncpdp'),
#     col('currawp'),
#     col('currwac'),
#     col('ltc_clm'),
#     col('claims'),
#     col('state'),
#     col('tag.claimnbr'),
#     col('lob_biz_type'),
#     col('tag.carrier'),
#     col('tag.account'),
#     col('tag.group'),
#     col('speclock'),
#     col('drugname'),
#     col('srx_drug_flg'),
#     col('awp'),
#     col('lob'),
#     col('tag.dlvry_systm_cd'),
#     col('year').alias("report_year"),
#     col('month').alias("report_month"),
#     col('mbr_gid').alias('memberid'),
#     col('tot_mbr'),
#     col('tag.orgnl_ingrd_cost'),
#     col('tag.clcfee'),
#     col('wac_amt'),
#     col('preshift.dayssply'),
#     col('tag.mc'),
#     when(col("tag.dlvry_systm_cd") != 'M', col("preshift.dayssply")).otherwise(lit(0)).alias('retail_ds'),
#     col('rxnum'),
#     current_timestamp().alias("current_timestamp"))
# )

# COMMAND ----------

#display(df_rpt_base)
#df_rpt_base.printSchema()

# COMMAND ----------

# MAGIC %md ###Top Drugs Report

# COMMAND ----------

# ### Only execute this cell when the first 'if' condition is satisfied

# if "uwdfm_t_cc_topdrugs_rpt" not in tblList or process_time_delta_df.count()!=0:

#   if "uwdfm_t_cc_topdrugs_rpt" in tblList:
#     uwdfm_t_cc_topdrugs_rpt_df = (
#     df_rpt_base.groupBy(col('parent_request_id'), 
#                       col('req_gid'),
#                       col('task_id'),
#                       col('lob'),
#                       col('lob_biz_type'),
#                       col('client_nm'),
#                       col('carrier'),
#                       col('account'),
#                       col('group'),
#                       col('speclock'),
#                       col('drugname'),
#                       col('genbrand'))
#     .agg(F.sum(col('awp')).alias('awpsum'))
#     .withColumn("current_timestamp",current_timestamp())
#   )
#   else:
#     uwdfm_t_cc_topdrugs_rpt_df = (
#     df_rpt_base2.groupBy(col('parent_request_id'), 
#                       col('req_gid'),
#                       col('task_id'),
#                       col('lob'),
#                       col('lob_biz_type'),
#                       col('client_nm'),
#                       col('carrier'),
#                       col('account'),
#                       col('group'),
#                       col('speclock'),
#                       col('drugname'),
#                       col('genbrand'))
#   .agg(F.sum(col('awp')).alias('awpsum'))
#   .withColumn("current_timestamp",current_timestamp())
#   )   
    
#   if "uwdfm_t_cc_topdrugs_rpt" in tblList:
#     print('delta write')
#     if env1 ==1: ### Only do the merge/delete for test requests in DEV/QA
#     #if 1==1:
#       process_time_delta_df.createOrReplaceTempView("TopDrugs1")
#       TopDrugs2_df = spark.read.format("delta").load("dbfs:/mnt/bronze/afp/dfm/reporting/uwdfm_t_cc_topdrugs_rpt").createOrReplaceTempView("TopDrugs2")
#       CommonTopDrugs_df=spark.sql("""
#       Select
#       S2.parent_request_id,
#       S2.req_gid,
#       S2.task_id
#       from TopDrugs2 S2 
#       join TopDrugs1 S1 on S2.parent_request_id = S1.uw_req_id and S2.req_gid = S1.req_gid and S2.task_id = S1.cpt_id
#       where S2.parent_request_id like('*req_gid')
#       """).createOrReplaceTempView("CommonTopDrugs")
#       query = "MERGE INTO delta.`dbfs:/mnt/bronze/afp/dfm/reporting/uwdfm_t_cc_topdrugs_rpt` AS d \
#               using (SELECT * FROM CommonTopDrugs) AS k \
#               ON d.parent_request_id = k.parent_request_id \
#               AND d.req_gid = k.req_gid \
#               AND d.task_id = k.task_id \
#               WHEN MATCHED THEN DELETE"
#       sqlContext.sql(query)

#     uwdfm_t_cc_topdrugs_rpt_df.write \
#    .format("delta") \
#    .partitionBy("parent_request_id","req_gid","task_id") \
#    .mode("append") \
#    .option("mergeSchema", "true") \
#    .option("path",dfm_reporting_path+"uwdfm_t_cc_topdrugs_rpt")\
#    .saveAsTable("reporting.uwdfm_t_cc_topdrugs_rpt")


#   else:
#     print('first write')
#     uwdfm_t_cc_topdrugs_rpt_df.write \
#    .format("delta") \
#    .partitionBy("parent_request_id","req_gid","task_id") \
#    .mode("append") \
#    .option("mergeSchema", "true") \
#    .option("path",dfm_reporting_path+"uwdfm_t_cc_topdrugs_rpt")\
#    .saveAsTable("reporting.uwdfm_t_cc_topdrugs_rpt")

# COMMAND ----------

# MAGIC %md ###State Report

# COMMAND ----------

# ### Only execute this cell when the first 'if' condition is satisfied

# if "uwdfm_t_cc_state_rpt" not in tblList or process_time_delta_df.count()!=0:

#   if "uwdfm_t_cc_state_rpt" in tblList:
#     uwdfm_t_cc_state_rpt_df = (
#     df_rpt_base
#    .groupBy(col('parent_request_id'), 
#            col('req_gid'),
#            col('task_id'),
#            col('lob'),
#            col('lob_biz_type'),
#            col('client_nm'),
#            col('carrier'),
#            col('account'),
#            col('group'),
#            col('state')
#           )
#   .agg(F.count(col('claimnbr')).alias('clm_count'))
#   .withColumn("current_timestamp",current_timestamp())
#  )
#   else:
#     uwdfm_t_cc_state_rpt_df = (
#     df_rpt_base2
#    .groupBy(col('parent_request_id'), 
#            col('req_gid'),
#            col('task_id'),
#            col('lob'),
#            col('lob_biz_type'),
#            col('client_nm'),
#            col('carrier'),
#            col('account'),
#            col('group'),
#            col('state')
#           )
#   .agg(F.count(col('claimnbr')).alias('clm_count'))
#   .withColumn("current_timestamp",current_timestamp())
#  )

#   if  "uwdfm_t_cc_state_rpt" in tblList:
#     print('delta write')
#     if env1 ==1: ### Only do the merge/delete for test requests in DEV/QA
#     #if 1==1:
#       process_time_delta_df.createOrReplaceTempView("StateReport1")
#       StateReport2_df = spark.read.format("delta").load("dbfs:/mnt/bronze/afp/dfm/reporting/uwdfm_t_cc_state_rpt").createOrReplaceTempView("StateReport2")
#       CommonStateReport_df=spark.sql("""
#       Select
#       S2.parent_request_id,
#       S2.req_gid,
#       S2.task_id
#       from StateReport2 S2 
#       join StateReport1 S1 on S2.parent_request_id = S1.uw_req_id and S2.req_gid = S1.req_gid and S2.task_id = S1.cpt_id
#       """).createOrReplaceTempView("CommonStateReport")
#       query = "MERGE INTO delta.`dbfs:/mnt/bronze/afp/dfm/reporting/uwdfm_t_cc_state_rpt` AS d \
#               using (SELECT * FROM CommonStateReport) AS k \
#               ON d.parent_request_id = k.parent_request_id \
#               AND d.req_gid = k.req_gid \
#               AND d.task_id = k.task_id \
#               WHEN MATCHED THEN DELETE"
#       sqlContext.sql(query)

#     uwdfm_t_cc_state_rpt_df.write \
#    .format("delta") \
#    .partitionBy("parent_request_id","req_gid","task_id") \
#    .mode("append") \
#    .option("mergeSchema", "true") \
#    .option("path",dfm_reporting_path+"uwdfm_t_cc_state_rpt")\
#    .saveAsTable("reporting.uwdfm_t_cc_state_rpt")


#   else:
#     print('first write')
#     uwdfm_t_cc_state_rpt_df.write \
#    .format("delta") \
#    .partitionBy("parent_request_id","req_gid","task_id") \
#    .mode("append") \
#    .option("mergeSchema", "true") \
#    .option("path",dfm_reporting_path + "uwdfm_t_cc_state_rpt")\
#    .saveAsTable("reporting.uwdfm_t_cc_state_rpt")

# COMMAND ----------

#display(uwdfm_t_cc_state_rpt_df)

# COMMAND ----------

# MAGIC %md ###Pre-shift Report

# COMMAND ----------

# ### Only execute this cell when the first 'if' condition is satisfied

# if "uwdfm_t_cc_preshift_rpt" not in tblList or process_time_delta_df.count()!=0:

#   if "uwdfm_t_cc_preshift_rpt" in tblList:
#     uwdfm_t_cc_preshift_rpt_df = (
#     df_rpt_base.withColumn('dofii_mm', F.trunc(col('dofii'), 'mm')) 
# 		.groupBy(col('parent_request_id'), col('req_gid'), col('task_id'), col('lob'), 
#                  col('client_nm'), col('carrier'),col('account'), col('group'),col('dlvry_systm_cd'),
#                  col('report_year'),col('report_month'), col('dofii_mm'),col('lob_biz_type'))
# 		.agg(
#           col('dofii_mm').alias('dof'),
#           when(col('dlvry_systm_cd') == 'M', countDistinct(col('claimnbr'))).alias('clmcnt_dlvry_m'),
#           when(col('dlvry_systm_cd') == 'R', countDistinct(col('claimnbr'))).alias('clmcnt_dlvry_r'),
#           when(col('dlvry_systm_cd') == 'M', countDistinct(col('memberid'))).alias('mbrcnt_yr_dlvry_m'),
#           when(col('dlvry_systm_cd') == 'R', countDistinct(col('memberid'))).alias('mbrcnt_yr_dlvry_r'),
#           countDistinct(col('memberid')).alias('mbrcnt_yr'),
#           when(col('dlvry_systm_cd') == 'M', countDistinct(col('tot_mbr'))).alias('ephcnt_yr_dlvry_m'),
#           when(col('dlvry_systm_cd') == 'R', countDistinct(col('tot_mbr'))).alias('ephcnt_yr_dlvry_r'),
#           countDistinct(col('memberid')).alias('ephcnt_yr'),
#           countDistinct(col('memberid')).alias('mbrcount_month'),
#           countDistinct(col('tot_mbr')).alias('ephcount_month'),
#           F.sum(col('orgnl_ingrd_cost')).alias('ingrd_cost'),
#           F.sum(col('clcfee')).alias('clcfee'),
#           count(col('claimnbr')).alias('clmcount'),
#           current_timestamp().alias("current_timestamp")
#         )
#  )
#   else:
#     uwdfm_t_cc_preshift_rpt_df = (
#     df_rpt_base2.withColumn('dofii_mm', F.trunc(col('dofii'), 'mm')) 
# 		.groupBy(col('parent_request_id'), col('req_gid'), col('task_id'), col('lob'), 
#                  col('client_nm'), col('carrier'),col('account'), col('group'),col('dlvry_systm_cd'),
#                  col('report_year'),col('report_month'), col('dofii_mm'),col('lob_biz_type'))
# 		.agg(
#           col('dofii_mm').alias('dof'),
#           when(col('dlvry_systm_cd') == 'M', countDistinct(col('claimnbr'))).alias('clmcnt_dlvry_m'),
#           when(col('dlvry_systm_cd') == 'R', countDistinct(col('claimnbr'))).alias('clmcnt_dlvry_r'),
#           when(col('dlvry_systm_cd') == 'M', countDistinct(col('memberid'))).alias('mbrcnt_yr_dlvry_m'),
#           when(col('dlvry_systm_cd') == 'R', countDistinct(col('memberid'))).alias('mbrcnt_yr_dlvry_r'),
#           countDistinct(col('memberid')).alias('mbrcnt_yr'),
#           when(col('dlvry_systm_cd') == 'M', countDistinct(col('tot_mbr'))).alias('ephcnt_yr_dlvry_m'),
#           when(col('dlvry_systm_cd') == 'R', countDistinct(col('tot_mbr'))).alias('ephcnt_yr_dlvry_r'),
#           countDistinct(col('memberid')).alias('ephcnt_yr'),
#           countDistinct(col('memberid')).alias('mbrcount_month'),
#           countDistinct(col('tot_mbr')).alias('ephcount_month'),
#           F.sum(col('orgnl_ingrd_cost')).alias('ingrd_cost'),
#           F.sum(col('clcfee')).alias('clcfee'),
#           count(col('claimnbr')).alias('clmcount'),
#           current_timestamp().alias("current_timestamp")
#         )
#  )

#   if  "uwdfm_t_cc_preshift_rpt" in tblList:
#     print('delta write')
#     if env1 ==1: ### Only do the merge/delete for test requests in DEV/QA
#     #if 1==1:
#       process_time_delta_df.createOrReplaceTempView("PreShift1")
#       PreShift2_df = spark.read.format("delta").load("dbfs:/mnt/bronze/afp/dfm/reporting/uwdfm_t_cc_preshift_rpt").createOrReplaceTempView("PreShift2")
#       CommonPreShift_df=spark.sql("""
#       Select
#       S2.parent_request_id,
#       S2.req_gid,
#       S2.task_id
#       from PreShift2 S2 
#       join PreShift1 S1 on S2.parent_request_id = S1.uw_req_id and S2.req_gid = S1.req_gid and S2.task_id = S1.cpt_id
#       """).createOrReplaceTempView("CommonPreShift")
#       query = "MERGE INTO delta.`dbfs:/mnt/bronze/afp/dfm/reporting/uwdfm_t_cc_preshift_rpt` AS d \
#               using (SELECT * FROM CommonPreShift) AS k \
#               ON d.parent_request_id = k.parent_request_id \
#               AND d.req_gid = k.req_gid \
#               AND d.task_id = k.task_id \
#               WHEN MATCHED THEN DELETE"
#       sqlContext.sql(query)

#     uwdfm_t_cc_preshift_rpt_df.write \
#    .format("delta") \
#    .partitionBy("parent_request_id","req_gid","task_id") \
#    .mode("append") \
#    .option("mergeSchema", "true") \
#    .option("path",dfm_reporting_path+"uwdfm_t_cc_preshift_rpt")\
#    .saveAsTable("reporting.uwdfm_t_cc_preshift_rpt")


#   else:
#     print('first write')
#     uwdfm_t_cc_preshift_rpt_df.write \
#    .format("delta") \
#    .partitionBy("parent_request_id","req_gid","task_id") \
#    .mode("append") \
#    .option("mergeSchema", "true") \
#    .option("path",dfm_reporting_path + "uwdfm_t_cc_preshift_rpt")\
#    .saveAsTable("reporting.uwdfm_t_cc_preshift_rpt")

# COMMAND ----------

#display(uwdfm_t_cc_preshift_rpt_df)

# COMMAND ----------

# MAGIC %md ###Summary Report

# COMMAND ----------

# ### Only execute this cell when the first 'if' condition is satisfied

# if "uwdfm_t_cc_summary_rpt" not in tblList or process_time_delta_df.count()!=0:
  
#   if "uwdfm_t_cc_summary_rpt" not in tblList:
#     df_rpt_summary = df_rpt_base2
#   else:
#     df_rpt_summary = df_rpt_base
  

#   def get_summary(qc_metric,metric,condition,df_rpt_summary):
#     df = (df_rpt_summary
#       .where(condition)
#       .withColumn("qc_metrics",lit(qc_metric))
#       .withColumn("metric",metric)
#       .withColumn("genbrand_new",when(col('genbrand')=='B','BRAND').when(col('genbrand')=='G','GENERIC'))
#       .groupBy('qc_metrics',"metric","genbrand_new")
#       .agg(
#         countDistinct(col('rxnum')).alias('rxnumcnt'),
#         count(col('awp')).alias('awpsum'),
#         avg(col('awp')).alias('avgawp'),
#         avg(col('wac_amt')).alias('avgwac'),
#         avg(col('dayssply')).alias('avgdays')
#       )
#      )
#     return df


#   df_1 = get_summary("MAIL/RETAIL",col('dlvry_systm_cd'),"1==1",df_rpt_summary)
#   df_2 = get_summary("MC",col('MC'),"1==1",df_rpt_summary)
#   df_3 = get_summary("RETAIL_30_90",when(col('retail_ds')>='84','RETAIL 90').when(col('retail_ds')<'34',"RETAIL 30"),"(dlvry_systm_cd=='R')",df_rpt_summary)
#   df_4 = get_summary("RETAIL/MC",lit('1'),"(dlvry_systm_cd=='R') or (mc =='1') ",df_rpt_summary)
#   df_5 = get_summary("MAIL/MC",lit('1'),"(dlvry_systm_cd=='M') or (mc =='1') ",df_rpt_summary)
#   df_6 = get_summary("LTC",col('ltc_clm'),"(ltc_clm=='1') ",df_rpt_summary)

#   summary_df = df_1.union(df_2).union(df_3).union(df_4).union(df_5).union(df_6)

#   agg_df = (df_rpt_summary
#           .groupBy(col('parent_request_id'),col('req_gid'), col('task_id'))
#           .agg(countDistinct(col('rxnum')).alias('totrxnumcnt'),F.sum(col('awp')).alias("awptotal"))
#          )

#   uwdfm_t_cc_summary_rpt_df = summary_df.crossJoin(agg_df).withColumn("current_timestamp",current_timestamp())


#   if "uwdfm_t_cc_summary_rpt" in tblList:
#     print('delta write')
#     if env1 ==1: ### Only do the merge/delete for test requests in DEV/QA
#     #if 1==1:
#       process_time_delta_df.createOrReplaceTempView("Summary1")
#       Summary2_df = spark.read.format("delta").load("dbfs:/mnt/bronze/afp/dfm/reporting/uwdfm_t_cc_summary_rpt").createOrReplaceTempView("Summary2")
#       CommonSummary_df=spark.sql("""
#       Select
#       S2.parent_request_id,
#       S2.req_gid,
#       S2.task_id
#       from Summary2 S2 
#       join Summary1 S1 on S2.parent_request_id = S1.uw_req_id and S2.req_gid = S1.req_gid and S2.task_id = S1.cpt_id
#       """).createOrReplaceTempView("CommonSummary")
#       query = "MERGE INTO delta.`dbfs:/mnt/bronze/afp/dfm/reporting/uwdfm_t_cc_summary_rpt` AS d \
#               using (SELECT * FROM CommonSummary) AS k \
#               ON d.parent_request_id = k.parent_request_id \
#               AND d.req_gid = k.req_gid \
#               AND d.task_id = k.task_id \
#               WHEN MATCHED THEN DELETE"
#       sqlContext.sql(query)

#     uwdfm_t_cc_summary_rpt_df.write \
#    .format("delta") \
#    .partitionBy("parent_request_id","req_gid","task_id") \
#    .mode("append") \
#    .option("mergeSchema", "true") \
#    .option("path",dfm_reporting_path+"uwdfm_t_cc_summary_rpt")\
#    .saveAsTable("reporting.uwdfm_t_cc_summary_rpt")


#   else:
#     print('first write')
#     uwdfm_t_cc_summary_rpt_df.write \
#    .format("delta") \
#    .partitionBy("parent_request_id","req_gid","task_id") \
#    .mode("append") \
#    .option("mergeSchema", "true") \
#    .option("path",dfm_reporting_path + "uwdfm_t_cc_summary_rpt")\
#    .saveAsTable("reporting.uwdfm_t_cc_summary_rpt")

# COMMAND ----------

#uwdfm_t_cc_summary_rpt_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md ###Formulary Shifting Report

# COMMAND ----------

# ### Only execute this cell when the first 'if' condition is satisfied

# if "uwdfm_t_formulary_shifted_rpt" not in tblList or process_time_delta_df.count()!=0:

#   if "uwdfm_t_formulary_shifted_rpt" in tblList:
#     process_time_delta_df_Formulary1=spark.sql("""
#     Select 
#     uw_req_id,req_gid,cpt_id
#     from processTime3
#     where 1=1
#     and concat(uw_req_id,req_gid,cpt_id) NOT IN
#     (Select
#     concat(uw_req_id,req_gid,cpt_id)
#     from process_time_initial)
#     and client_description is not null
#     """)
#   else:
#     process_time_delta_df_Formulary1=spark.sql("""
#     Select 
#     uw_req_id,req_gid,cpt_id--,client_description
#     from processTime3
#     where client_description is not null
#     """)

#   uwdfm_t_formulary_shifted_df = read_output("out_uwdfm_t_formulary_shifted").alias('oufs').join(process_time_delta_df_Formulary1.alias('ptd'),(col('oufs.parent_request_id') ==                   col('ptd.uw_req_id'))& (col('oufs.req_gid') ==  col('ptd.req_gid')) & (col('oufs.task_id') ==  col('ptd.cpt_id')),'inner')
 
#   clm_cnt_origclaim=(uwdfm_t_formulary_shifted_df.groupBy("parent_request_id","oufs.req_gid","task_id","origclaim").agg(F.countDistinct("claimnbr").alias("count"),F.sum("claims") \
#                     .alias("sum")))

#   shifted_rpt = (
#   uwdfm_t_formulary_shifted_df
#   .groupBy("parent_request_id","oufs.req_gid","task_id","prop_frmly_id","prop_ntwrk","origclaim","year","month")
#   .agg(F.sum("currawp").alias("sum_currawp"),
#         F.sum("currwac").alias("sum_currwac"),
#         F.sum("currnadac").alias("sum_currnadac"),
#         F.sum("claims").alias("sum_claims"),
#         F.sum("adjawp").alias("sum_adjawp"),
#         F.sum("adjwac").alias("sum_adjwac"),
#         F.avg("dayssply").alias("avg_days_supply")
#       )
# ) 

#   uwdfm_t_formulary_shifted_rpt_df = (
#   shifted_rpt.alias("a")
#   .join(clm_cnt_origclaim.alias("b"),
#         (col("a.req_gid") == col("b.req_gid"))
#         & (col("a.task_id") == col("b.task_id"))
#         & (col("a.origclaim") == col("b.origclaim")) ,"left")
#   .select(
#     col("a.parent_request_id"),
#     col("a.req_gid"),
#     col("a.task_id"),
#     col("a.prop_frmly_id"),
#     col("a.prop_ntwrk"),
#     col("a.origclaim"),
#     col("a.year").alias("report_year"),
#     col("a.month").alias("report_month"),
#     when(col("b.origclaim")=='Y',col("b.count")).alias("clm_cnt_origclaim_y"),
#     when(col("b.origclaim")=='Y',col("b.sum")).alias("claims_sum_origclaim_y"),
#     when(col("b.origclaim")=='N',col("b.count")).alias("clm_cnt_origclaim_n"),
#     when(col("b.origclaim")=='N',col("b.sum")).alias("claims_sum_origclaim_n"),
#     when(col("b.origclaim")=='P',col("b.count")).alias("clm_cnt_origclaim_p"),
#     when(col("b.origclaim")=='P',col("b.sum")).alias("claims_sum_origclaim_p"),
#     col("a.sum_currawp"),
#     col("a.sum_currwac"),
#     col("a.sum_currnadac"),
#     col("a.sum_claims"),
#     col("a.sum_adjawp"),
#     col("a.sum_adjwac"),
#     col("a.avg_days_supply"),
#   ).withColumn("load_ts", lit(F.current_timestamp()) )
  
# )

#   if  "uwdfm_t_formulary_shifted_rpt" in tblList:
#     print('delta write')
#     if env1 ==1: ### Only do the merge/delete for test requests in DEV/QA
#     #if 1==1:
#       process_time_delta_df_Formulary1.createOrReplaceTempView("Formulary1")
#       Formulary2_df = spark.read.format("delta").load("dbfs:/mnt/bronze/afp/dfm/reporting/uwdfm_t_formulary_shifted_rpt").createOrReplaceTempView("Formulary2")
#       CommonFormulary_df=spark.sql("""
#       Select
#       S2.parent_request_id,
#       S2.req_gid,
#       S2.task_id
#       from Formulary2 S2 
#       join Formulary1 S1 on S2.parent_request_id = S1.uw_req_id and S2.req_gid = S1.req_gid and S2.task_id = S1.cpt_id
#       """).createOrReplaceTempView("CommonFormulary")
#       query = "MERGE INTO delta.`dbfs:/mnt/bronze/afp/dfm/reporting/uwdfm_t_formulary_shifted_rpt` AS d \
#               using (SELECT * FROM CommonFormulary) AS k \
#               ON d.parent_request_id = k.parent_request_id \
#               AND d.req_gid = k.req_gid \
#               AND d.task_id = k.task_id \
#               WHEN MATCHED THEN DELETE"
#       sqlContext.sql(query)

#     uwdfm_t_formulary_shifted_rpt_df.write \
#    .format("delta") \
#    .partitionBy("parent_request_id","req_gid","task_id") \
#    .mode("append") \
#    .option("mergeSchema", "true") \
#    .option("path",dfm_reporting_path+"uwdfm_t_formulary_shifted_rpt")\
#    .saveAsTable("reporting.uwdfm_t_formulary_shifted_rpt")


#   else:
#     print('first write')
#     uwdfm_t_formulary_shifted_rpt_df.write \
#    .format("delta") \
#    .partitionBy("parent_request_id","req_gid","task_id") \
#    .mode("append") \
#    .option("mergeSchema", "true") \
#    .option("path",dfm_reporting_path + "uwdfm_t_formulary_shifted_rpt")\
#    .saveAsTable("reporting.uwdfm_t_formulary_shifted_rpt")
    
#   #uwdfm_t_formulary_shifted_df.cache()

# COMMAND ----------

#display(uwdfm_t_formulary_shifted_rpt_df)

# COMMAND ----------

# ### Only execute this cell when the first 'if' condition is satisfied

# if "uwdfm_t_formulary_shifted_rpt_excl" not in tblList or process_time_delta_df.count()!=0:

#   if "uwdfm_t_formulary_shifted_rpt_excl" in tblList:
#     process_time_delta_df_Formulare1=spark.sql("""
#     Select 
#     uw_req_id,req_gid,cpt_id
#     from processTime3
#     where 1=1
#     and concat(uw_req_id,req_gid,cpt_id) NOT IN
#     (Select
#     concat(uw_req_id,req_gid,cpt_id)
#     from process_time_initial)
#     and client_description is not null
#     """)
#   else:
#     process_time_delta_df_Formulare1=spark.sql("""
#     Select 
#     uw_req_id,req_gid,cpt_id--,client_description
#     from processTime3
#     where client_description is not null
#     """)

#   formulary_shifted_rpt_excl_df = (
#   uwdfm_t_formulary_shifted_df
#   .filter(col("origclaim") != 'P')
#   .groupBy("parent_request_id","oufs.req_gid","task_id","lob","prop_frmly_id","prop_ntwrk","gpi","b_gpi",
#            "origclaim","drugname","prop_frmly_name","curr_frmly_id")
#   .agg(F.sum("shiftaway").alias("shiftaway"),
#         F.sum("sfactor").alias("sfactor"),
#         F.sum("claims").alias("claims"),
#         F.sum("dayssply").alias("dayssply"),
#         F.sum("currawp").alias("currawp"),
#         F.sum("currwac").alias("currwac"),
#         F.avg("currnadac").alias("currnadac")
#       )
#   .withColumn("current_timestamp",current_timestamp())
# )

#   if  "uwdfm_t_formulary_shifted_rpt_excl" in tblList:
#     print('delta write')
#     if env1 ==1: ### Only do the merge/delete for test requests in DEV/QA
#     #if 1==1:
#       process_time_delta_df_Formulare1.createOrReplaceTempView("Formulare1")
#       Formulare2_df = spark.read.format("delta").load("dbfs:/mnt/bronze/afp/dfm/reporting/uwdfm_t_formulary_shifted_rpt_excl").createOrReplaceTempView("Formulare2")
#       CommonFormulare_df=spark.sql("""
#       Select
#       S2.parent_request_id,
#       S2.req_gid,
#       S2.task_id
#       from Formulare2 S2 
#       join Formulare1 S1 on S2.parent_request_id = S1.uw_req_id and S2.req_gid = S1.req_gid and S2.task_id = S1.cpt_id
#       """).createOrReplaceTempView("CommonFormulare")
#       query = "MERGE INTO delta.`dbfs:/mnt/bronze/afp/dfm/reporting/uwdfm_t_formulary_shifted_rpt_excl` AS d \
#               using (SELECT * FROM CommonFormulare) AS k \
#               ON d.parent_request_id = k.parent_request_id \
#               AND d.req_gid = k.req_gid \
#               AND d.task_id = k.task_id \
#               WHEN MATCHED THEN DELETE"
#       sqlContext.sql(query)

#     formulary_shifted_rpt_excl_df.write \
#    .format("delta") \
#    .partitionBy("parent_request_id","req_gid","task_id") \
#    .mode("append") \
#    .option("mergeSchema", "true") \
#    .option("path",dfm_reporting_path+"uwdfm_t_formulary_shifted_rpt_excl")\
#    .saveAsTable("reporting.uwdfm_t_formulary_shifted_rpt_excl")


#   else:
#     print('first write')
#     formulary_shifted_rpt_excl_df.write \
#    .format("delta") \
#    .partitionBy("parent_request_id","req_gid","task_id") \
#    .mode("append") \
#    .option("mergeSchema", "true") \
#    .option("path",dfm_reporting_path + "uwdfm_t_formulary_shifted_rpt_excl")\
#    .saveAsTable("reporting.uwdfm_t_formulary_shifted_rpt_excl")

# COMMAND ----------

# display(formulary_shifted_rpt_excl_df)

# COMMAND ----------

# MAGIC %md ###HG Shifting Report

# COMMAND ----------

# ### Only execute this cell when the first 'if' condition is satisfied

# if "uwdfm_t_hg_shifted_rpt" not in tblList or process_time_delta_df.count()!=0:

#   if "uwdfm_t_hg_shifted_rpt" in tblList:
#     process_time_delta_df_HG1=spark.sql("""
#     Select 
#     uw_req_id,req_gid,cpt_id
#     from processTime3
#     where 1=1
#     and concat(uw_req_id,req_gid,cpt_id) NOT IN
#     (Select
#     concat(uw_req_id,req_gid,cpt_id)
#     from process_time_initial)
#     and client_description is not null
#     """)
#   else:
#     process_time_delta_df_HG1=spark.sql("""
#     Select 
#     uw_req_id,req_gid,cpt_id--,client_description
#     from processTime3
#     where client_description is not null
#     """)

#   hg_shifted_df = (read_output("out_uwdfm_t_hg_shifted")).alias('ouhs').join(process_time_delta_df_HG1.alias('ptd'),(col('ouhs.parent_request_id') ==  col('ptd.uw_req_id'))&    
#   (col('ouhs.req_gid') ==  col('ptd.req_gid')) & (col('ouhs.task_id') ==  col('ptd.cpt_id')),'inner')
  
#   clm_hg_origclaim =(hg_shifted_df.groupBy("parent_request_id","ouhs.req_gid","task_id","hg_origclaim").agg(F.countDistinct("claimnbr").alias("count"),F.sum("claims").alias("sum")))

#   rpt_df = (
#   hg_shifted_df
#   .groupBy("parent_request_id","ouhs.req_gid","task_id","prop_frmly_id","prop_ntwrk","hg_origclaim","year","month")
#   .agg(
#         F.sum("currawp").alias("sum_currawp"),
#         F.sum("currwac").alias("sum_currwac"),
#         F.sum("currnadac").alias("sum_currnadac"),
#         F.sum("claims").alias("sum_claims"),
#         F.sum("adjawp").alias("sum_adjawp"),
#         F.sum("adjwac").alias("sum_adjwac"),
#         F.avg("dayssply").alias("avg_days_supply")
#       )
# )

#   uwdfm_t_hg_shifted_rpt_df = (
#   rpt_df.alias("a")
#   .join(clm_hg_origclaim.alias("b"),
#         (col("a.parent_request_id") == col("b.parent_request_id"))
#         & (col("a.req_gid") == col("b.req_gid"))
#         & (col("a.task_id") == col("b.task_id"))
#         & (col("a.hg_origclaim") == col("b.hg_origclaim")) ,"left")
#   .select(
#     col("a.parent_request_id"),
#     col("a.req_gid"),
#     col("a.task_id"),
#     col("a.prop_frmly_id"),
#     col("a.prop_ntwrk"),
#     col("a.hg_origclaim"),
#     col("a.year").alias("report_year"),
#     col("a.month").alias("report_month"),
#     when(col("b.hg_origclaim")=='Y',col("b.count")).alias("clm_cnt_hgorigclaim_y"),
#     when(col("b.hg_origclaim")=='Y',col("b.sum")).alias("claims_sum_hgorigclaim_y"),
#     when(col("b.hg_origclaim")=='N',col("b.count")).alias("clm_cnt_hgorigclaim_n"),
#     when(col("b.hg_origclaim")=='N',col("b.sum")).alias("claims_sum_hgorigclaim_n"),
#     when(col("b.hg_origclaim")=='P',col("b.count")).alias("clm_cnt_hgorigclaim_p"),
#     when(col("b.hg_origclaim")=='P',col("b.sum")).alias("claims_sum_hgorigclaim_p"),
#     col("a.sum_currawp"),
#     col("a.sum_currwac"),
#     col("a.sum_currnadac"),
#     col("a.sum_claims"),
#     col("a.sum_adjawp"),
#     col("a.sum_adjwac"),
#     col("a.avg_days_supply"),
#   ).withColumn("load_ts", lit(F.current_timestamp()) )
# )


#   if  "uwdfm_t_hg_shifted_rpt" in tblList:
#     print('delta write')
#     if env1 ==1: ### Only do the merge/delete for test requests in DEV/QA
#     #if 1==1:
#       process_time_delta_df_HG1.createOrReplaceTempView("HG1")
#       HG2_df = spark.read.format("delta").load("dbfs:/mnt/bronze/afp/dfm/reporting/uwdfm_t_hg_shifted_rpt").createOrReplaceTempView("HG2")
#       CommonHG_df=spark.sql("""
#       Select
#       S2.parent_request_id,
#       S2.req_gid,
#       S2.task_id
#       from HG2 S2 
#       join HG1 S1 on S2.parent_request_id = S1.uw_req_id and S2.req_gid = S1.req_gid and S2.task_id = S1.cpt_id
#       """).createOrReplaceTempView("CommonHG")
#       query = "MERGE INTO delta.`dbfs:/mnt/bronze/afp/dfm/reporting/uwdfm_t_hg_shifted_rpt` AS d \
#               using (SELECT * FROM CommonHG) AS k \
#               ON d.parent_request_id = k.parent_request_id \
#               AND d.req_gid = k.req_gid \
#               AND d.task_id = k.task_id \
#               WHEN MATCHED THEN DELETE"
#       sqlContext.sql(query)

#     uwdfm_t_hg_shifted_rpt_df.write \
#    .format("delta") \
#    .partitionBy("parent_request_id","req_gid","task_id") \
#    .mode("append") \
#    .option("mergeSchema", "true") \
#    .option("path",dfm_reporting_path+"uwdfm_t_hg_shifted_rpt")\
#    .saveAsTable("reporting.uwdfm_t_hg_shifted_rpt")


#   else:
#     print('first write')
#     uwdfm_t_hg_shifted_rpt_df.write \
#    .format("delta") \
#    .partitionBy("parent_request_id","req_gid","task_id") \
#    .mode("append") \
#    .option("mergeSchema", "true") \
#    .option("path",dfm_reporting_path + "uwdfm_t_hg_shifted_rpt")\
#    .saveAsTable("reporting.uwdfm_t_hg_shifted_rpt")

# COMMAND ----------

#display(uwdfm_t_hg_shifted_rpt_df)

# COMMAND ----------

# MAGIC %md ###DAW9 Shifting Report

# COMMAND ----------

# ### Only execute this cell when the first 'if' condition is satisfied

# if "uwdfm_t_daw9_shifted_rpt" not in tblList or process_time_delta_df.count()!=0:

#   if  "uwdfm_t_daw9_shifted_rpt" in tblList:
#     process_time_delta_df_DAW91=spark.sql("""
#     Select 
#     uw_req_id,req_gid,cpt_id
#     from processTime3
#     where 1=1
#     and concat(uw_req_id,req_gid,cpt_id) NOT IN
#     (Select
#     concat(uw_req_id,req_gid,cpt_id)
#     from process_time_initial)
#     and client_description is not null
#     """)
#   else:
#     process_time_delta_df_DAW91=spark.sql("""
#     Select 
#     uw_req_id,req_gid,cpt_id--,client_description
#     from processTime3
#     where client_description is not null
#     """)

#   uwdfm_t_daw9_shifted_rpt_df = ((read_output("out_uwdfm_t_daw9_shifted")).alias("a").join(process_time_delta_df_DAW91.alias('ptd'),(col('a.parent_request_id') ==  col('ptd.uw_req_id'))&   (col('a.req_gid') ==  col('ptd.req_gid')) & (col('a.task_id') ==  col('ptd.cpt_id')),'inner')
#                                  .groupby(col("a.req_gid"),
#                                           col("a.task_id"),
#                                           col("a.parent_request_id"),
#                                           col("a.prop_frmly_id"),
#                                           col("a.prop_ntwrk"),
#                                           col("a.daw9_origclaim"),
#                                           col("a.year"),
#                                           col("a.month"))
#                                  .agg(
#                                    when(
#                                         (col("daw9_origclaim") == 'Y'),
#                                         countDistinct(col("claimnbr"))).alias("clm_cnt_daw9_origclaim_y"),
#                                    when(
#                                         (col("daw9_origclaim") == 'N'),
#                                         countDistinct(col("claimnbr"))).alias("clm_cnt_daw9_origclaim_n"),
#                                    when(
#                                         (col("daw9_origclaim") == 'P'),
#                                         countDistinct(col("claimnbr"))).alias("clm_cnt_daw9_origclaim_p"),
#                                    when(
#                                         (col("daw9_origclaim") == 'Y'),
#                                         sparkSum(col("claims"))).alias("claims_sum_daw9_origclaim_y"),
#                                    when(
#                                        (col("daw9_origclaim") == 'N'),
#                                         sparkSum(col("claims"))).alias("claims_sum_daw9_origclaim_n"),
#                                    when(
#                                         (col("daw9_origclaim") == 'P'),
#                                         sparkSum(col("claims"))).alias("claims_sum_daw9_origclaim_p"),
#                                    sparkSum(col("currawp")).alias("sum_currawp"),
#                                    sparkSum(col("currwac")).alias("sum_currwac"),
#                                    sparkSum(col("currnadac")).alias("sum_currnadac"),
#                                    sparkSum(col("claims")).alias("sum_claims"),
#                                    sparkSum(col("adjawp")).alias("sum_adjawp"), 
#                                    sparkSum(col("adjwac")).alias("sum_adjwac"),
#                                    avg(col("dayssply")).alias("avg_days_supply")
#                                  )
#                                  .select(col("a.req_gid"),
#                                          col("a.task_id"),
#                                          col("a.parent_request_id"),
#                                          col("a.prop_frmly_id"),
#                                          col("a.prop_ntwrk"),
#                                          col("a.daw9_origclaim"),
#                                          col("a.year").alias("report_year"),
#                                          col("a.month").alias("report_month"),
#                                          current_timestamp().alias("load_ts"),
#                                          col("clm_cnt_daw9_origclaim_y"),
#                                          col("clm_cnt_daw9_origclaim_n"),
#                                          col("claims_sum_daw9_origclaim_y"),
#                                          col("claims_sum_daw9_origclaim_n"),
#                                          col("sum_currawp"),
#                                          col("sum_currwac"),
#                                          col("sum_currnadac"),
#                                          col("sum_claims"),
#                                          col("sum_adjawp"),
#                                          col("sum_adjwac"),
#                                          col("avg_days_supply")
#                                          )
#                                  )


#   if  "uwdfm_t_daw9_shifted_rpt" in tblList:
#     print('delta write')
#     if env1 ==1: ### Only do the merge/delete for test requests in DEV/QA
#     #if 1==1:
#       process_time_delta_df_DAW91.createOrReplaceTempView("DAW91")
#       DAW92_df = spark.read.format("delta").load("dbfs:/mnt/bronze/afp/dfm/reporting/uwdfm_t_daw9_shifted_rpt").createOrReplaceTempView("DAW92")
#       CommonDAW9_df=spark.sql("""
#       Select
#       S2.parent_request_id,
#       S2.req_gid,
#       S2.task_id
#       from DAW92 S2 
#       join DAW91 S1 on S2.parent_request_id = S1.uw_req_id and S2.req_gid = S1.req_gid and S2.task_id = S1.cpt_id
#       """).createOrReplaceTempView("CommonDAW9")
#       query = "MERGE INTO delta.`dbfs:/mnt/bronze/afp/dfm/reporting/uwdfm_t_daw9_shifted_rpt` AS d \
#               using (SELECT * FROM CommonDAW9) AS k \
#               ON d.parent_request_id = k.parent_request_id \
#               AND d.req_gid = k.req_gid \
#               AND d.task_id = k.task_id \
#               WHEN MATCHED THEN DELETE"
#       sqlContext.sql(query)

#     uwdfm_t_daw9_shifted_rpt_df.write \
#    .format("delta") \
#    .partitionBy("parent_request_id","req_gid","task_id") \
#    .mode("append") \
#    .option("mergeSchema", "true") \
#    .option("path",dfm_reporting_path+"uwdfm_t_daw9_shifted_rpt")\
#    .saveAsTable("reporting.uwdfm_t_daw9_shifted_rpt")


#   else:
#     print('first write')
#     uwdfm_t_daw9_shifted_rpt_df.write \
#    .format("delta") \
#    .partitionBy("parent_request_id","req_gid","task_id") \
#    .mode("append") \
#    .option("mergeSchema", "true") \
#    .option("path",dfm_reporting_path+"uwdfm_t_daw9_shifted_rpt")\
#    .saveAsTable("reporting.uwdfm_t_daw9_shifted_rpt")

# COMMAND ----------

#display(uwdfm_t_daw9_shifted_rpt_df)

# COMMAND ----------

# MAGIC %md ###PCSK9 Shifting Report

# COMMAND ----------

# ### Only execute this cell when the first 'if' condition is satisfied

# if "uwdfm_t_pcsk9_shifted_rpt" not in tblList or process_time_delta_df.count()!=0:

#   if  "uwdfm_t_pcsk9_shifted_rpt" in tblList:
#     process_time_delta_df_PCSK91=spark.sql("""
#     Select 
#     uw_req_id,req_gid,cpt_id
#     from processTime3
#     where 1=1
#     and concat(uw_req_id,req_gid,cpt_id) NOT IN
#     (Select
#     concat(uw_req_id,req_gid,cpt_id)
#     from process_time_initial)
#     and client_description is not null
#     """)
#   else:
#     process_time_delta_df_PCSK91=spark.sql("""
#     Select 
#     uw_req_id,req_gid,cpt_id--,client_description
#     from processTime3
#     where client_description is not null
#     """)

#   pcsk9_shifted_df = read_output("out_uwdfm_t_pcsk9_shifted").alias('oups').join(process_time_delta_df_PCSK91.alias('ptd'),(col('oups.parent_request_id') ==  col('ptd.uw_req_id'))&           (col('oups.req_gid') ==  col('ptd.req_gid')) & (col('oups.task_id') ==  col('ptd.cpt_id')),'inner')

#   clm_pcsk9_origclaim =  (
#   pcsk9_shifted_df
#   .groupBy("parent_request_id","oups.req_gid","task_id","daw9_origclaim")
#   .agg(F.countDistinct("claimnbr").alias("count"),F.sum("claims").alias("sum"))
# )

#   rpt_df = (
#   pcsk9_shifted_df
#   .groupBy("parent_request_id","oups.req_gid","task_id","prop_frmly_id","prop_ntwrk","daw9_origclaim","year","month")
#   .agg(
#         F.sum("currawp").alias("sum_currawp"),
#         F.sum("currwac").alias("sum_currwac"),
#         F.sum("currnadac").alias("sum_currnadac"),
#         F.sum("claims").alias("sum_claims"),
#         F.sum("adjawp").alias("sum_adjawp"),
#         F.sum("adjwac").alias("sum_adjwac"),
#         F.avg("dayssply").alias("avg_days_supply")
#       )
# )

#   uwdfm_t_pcsk9_shifted_rpt_df = (
#   rpt_df.alias("a")
#   .join(clm_pcsk9_origclaim.alias("b"),
#         (col("a.parent_request_id") == col("b.parent_request_id"))
#         & (col("a.req_gid") == col("b.req_gid"))
#         & (col("a.task_id") == col("b.task_id"))
#         & (col("a.daw9_origclaim") == col("b.daw9_origclaim")) ,"left")
#   .select(
#     col("a.parent_request_id"),
#     col("a.req_gid"),
#     col("a.task_id"),
#     col("a.prop_frmly_id"),
#     col("a.prop_ntwrk"),
#     col("a.daw9_origclaim").alias("origclaim"),
#     col("a.year").alias("report_year"),
#     col("a.month").alias("report_month"),
#     when(col("b.daw9_origclaim")=='Y',col("b.count")).alias("clm_cnt_pcsk9_origclaim_y"),
#     when(col("b.daw9_origclaim")=='Y',col("b.sum")).alias("claims_sum_pcsk9_origclaim_y"),
#     when(col("b.daw9_origclaim")=='N',col("b.count")).alias("clm_cnt_pcsk9_origclaim_n"),
#     when(col("b.daw9_origclaim")=='N',col("b.sum")).alias("claims_sum_pcsk9_origclaim_n"),
#     when(col("b.daw9_origclaim")=='P',col("b.count")).alias("clm_cnt_hgorigclaim_p"),
#     when(col("b.daw9_origclaim")=='P',col("b.sum")).alias("claims_sum_hgorigclaim_p"),
#     col("a.sum_currawp"),
#     col("a.sum_currwac"),
#     col("a.sum_currnadac"),
#     col("a.sum_claims"),
#     col("a.sum_adjawp"),
#     col("a.sum_adjwac"),
#     col("a.avg_days_supply"),
#   ).withColumn("load_ts", lit(F.current_timestamp()) )
# )

#   if  "uwdfm_t_pcsk9_shifted_rpt" in tblList:
#     print('delta write')
#     if env1 ==1: ### Only do the merge/delete for test requests in DEV/QA
#     #if 1==1:
#       process_time_delta_df_PCSK91.createOrReplaceTempView("PCSK91")
#       PCSK92_df = spark.read.format("delta").load("dbfs:/mnt/bronze/afp/dfm/reporting/uwdfm_t_pcsk9_shifted_rpt").createOrReplaceTempView("PCSK92")
#       Common_df=spark.sql("""
#       Select
#       S2.parent_request_id,
#       S2.req_gid,
#       S2.task_id
#       from PCSK92 S2 
#       join PCSK91 S1 on S2.parent_request_id = S1.uw_req_id and S2.req_gid = S1.req_gid and S2.task_id = S1.cpt_id
#       """).createOrReplaceTempView("CommonPCSK9")
#       query = "MERGE INTO delta.`dbfs:/mnt/bronze/afp/dfm/reporting/uwdfm_t_pcsk9_shifted_rpt` AS d \
#               using (SELECT * FROM CommonPCSK9) AS k \
#               ON d.parent_request_id = k.parent_request_id \
#               AND d.req_gid = k.req_gid \
#               AND d.task_id = k.task_id \
#               WHEN MATCHED THEN DELETE"
#       sqlContext.sql(query)

#     uwdfm_t_pcsk9_shifted_rpt_df.write \
#    .format("delta") \
#    .partitionBy("parent_request_id","req_gid","task_id") \
#    .mode("append") \
#    .option("mergeSchema", "true") \
#    .option("path",dfm_reporting_path+"uwdfm_t_pcsk9_shifted_rpt")\
#    .saveAsTable("reporting.uwdfm_t_pcsk9_shifted_rpt")


#   else:
#     print('first write')
#     uwdfm_t_pcsk9_shifted_rpt_df.write \
#    .format("delta") \
#    .partitionBy("parent_request_id","req_gid","task_id") \
#    .mode("append") \
#    .option("mergeSchema", "true") \
#    .option("path",dfm_reporting_path+"uwdfm_t_pcsk9_shifted_rpt")\
#    .saveAsTable("reporting.uwdfm_t_pcsk9_shifted_rpt")

# COMMAND ----------

#display(uwdfm_t_pcsk9_shifted_rpt_df)

# COMMAND ----------

# MAGIC %md ###Generic Pipeline Report

# COMMAND ----------

# ### Only execute this cell when the first 'if' condition is satisfied

# if "uwdfm_t_generic_pipeline_rpt" not in tblList or process_time_delta_df.count()!=0:

#   if  "uwdfm_t_generic_pipeline_rpt" in tblList:
#     process_time_delta_df_Generic1=spark.sql("""
#     Select
#     uw_req_id,req_gid,cpt_id
#     from processTime3
#     where 1=1
#     and concat(uw_req_id,req_gid,cpt_id) NOT IN
#     (Select
#     concat(uw_req_id,req_gid,cpt_id)
#     from process_time_initial)
#     and client_description is not null
#     """)
#   else:
#     process_time_delta_df_Generic1=spark.sql("""
#     Select
#     uw_req_id,req_gid,cpt_id--,client_description
#     from processTime3
#     where client_description is not null
#     """)

#   gp_shifted_df = read_output("out_uwdfm_t_generic_pipeline").alias('ougp').join(process_time_delta_df_Generic1.alias('ptd'),(col('ougp.parent_request_id') ==  col('ptd.uw_req_id'))&           (col('ougp.req_gid') ==  col('ptd.req_gid')) & (col('ougp.task_id') ==  col('ptd.cpt_id')),'inner')

#   uwdfm_t_generic_pipeline_rpt_df = gp_shifted_df.select(
#    col("parent_request_id"),
#     col("ougp.req_gid"),
#     col("task_id"),
#     col("prop_frmly_id"),
#     col("prop_ntwrk"),
#     col("dlvry_systm_cd"),
#     col("genbrand"),
#     col("year").alias("report_year"),
#     col("month").alias("report_month"),
#     F.sum('claims').over(Window.partitionBy("parent_request_id","ougp.req_gid","task_id","prop_frmly_id","prop_ntwrk","genbrand","dlvry_systm_cd","year","month")).alias('claims'),
#                                                                                                                             F.sum('claims_1').over(Window.partitionBy("parent_request_id","ougp.req_gid","task_id","prop_frmly_id","prop_ntwrk","genbrand","dlvry_systm_cd","year","month")).alias('claims_y1'),
#     F.sum('claims_2').over(Window.partitionBy("parent_request_id","ougp.req_gid","task_id","prop_frmly_id","prop_ntwrk","genbrand","dlvry_systm_cd","year","month")).alias('claims_y2'),
#     F.sum('claims_3').over(Window.partitionBy("parent_request_id","ougp.req_gid","task_id","prop_frmly_id","prop_ntwrk","genbrand","dlvry_systm_cd","year","month")).alias('claims_y3'),
#     F.sum('dayssply').over(Window.partitionBy("parent_request_id","ougp.req_gid","task_id","prop_frmly_id","prop_ntwrk","genbrand","dlvry_systm_cd","year","month")).alias('dayssply'),
#     F.sum('ds_sfted_1').over(Window.partitionBy("parent_request_id","ougp.req_gid","task_id","prop_frmly_id","prop_ntwrk","genbrand","dlvry_systm_cd","year","month")).alias('dayssply_1'),
#     F.sum('ds_sfted_2').over(Window.partitionBy("parent_request_id","ougp.req_gid","task_id","prop_frmly_id","prop_ntwrk","genbrand","dlvry_systm_cd","year","month")).alias('dayssply_2'),
#     F.sum('ds_sfted_3').over(Window.partitionBy("parent_request_id","ougp.req_gid","task_id","prop_frmly_id","prop_ntwrk","genbrand","dlvry_systm_cd","year","month")).alias('dayssply_3'),
#     F.avg('gsrrate_1').over(Window.partitionBy("parent_request_id","ougp.req_gid","task_id","prop_frmly_id","prop_ntwrk","genbrand","dlvry_systm_cd","year","month")).alias('avg_gsr1'),
#     F.avg('gsrrate_2').over(Window.partitionBy("parent_request_id","ougp.req_gid","task_id","prop_frmly_id","prop_ntwrk","genbrand","dlvry_systm_cd","year","month")).alias('avg_gsr2'),
#     F.avg('gsrrate_3').over(Window.partitionBy("parent_request_id","ougp.req_gid","task_id","prop_frmly_id","prop_ntwrk","genbrand","dlvry_systm_cd","year","month")).alias('avg_gsr3')
#   ).withColumn("load_ts", lit(F.current_timestamp()))

#   if  "uwdfm_t_generic_pipeline_rpt" in tblList:
#     print('delta write')
#     if env1 ==1: ### Only do the merge/delete for test requests in DEV/QA
#     #if 1==1:
#       process_time_delta_df_Generic1.createOrReplaceTempView("Generic1")
#       Generic2_df = spark.read.format("delta").load("dbfs:/mnt/bronze/afp/dfm/reporting/uwdfm_t_generic_pipeline_rpt").createOrReplaceTempView("Generic2")
#       CommonGeneric_df=spark.sql("""
#       Select
#       S2.parent_request_id,
#       S2.req_gid,
#       S2.task_id
#       from Generic2 S2
#       join Generic1 S1 on S2.parent_request_id = S1.uw_req_id and S2.req_gid = S1.req_gid and S2.task_id = S1.cpt_id
#       """).createOrReplaceTempView("CommonGeneric")
#       query = "MERGE INTO delta.`dbfs:/mnt/bronze/afp/dfm/reporting/uwdfm_t_generic_pipeline_rpt` AS d \
#               using (SELECT * FROM CommonGeneric) AS k \
#               ON d.parent_request_id = k.parent_request_id \
#               AND d.req_gid = k.req_gid \
#               AND d.task_id = k.task_id \
#               WHEN MATCHED THEN DELETE"
#       sqlContext.sql(query)

#     uwdfm_t_generic_pipeline_rpt_df.write \
#    .format("delta") \
#    .partitionBy("parent_request_id","req_gid","task_id") \
#    .mode("append") \
#    .option("mergeSchema", "true") \
#    .option("path",dfm_reporting_path+"uwdfm_t_generic_pipeline_rpt")\
#    .saveAsTable("reporting.uwdfm_t_generic_pipeline_rpt")


#   else:
#     print('first write')
#     uwdfm_t_generic_pipeline_rpt_df.write \
#    .format("delta") \
#    .partitionBy("parent_request_id","req_gid","task_id") \
#    .mode("append") \
#    .option("mergeSchema", "true") \
#    .option("path",dfm_reporting_path + "uwdfm_t_generic_pipeline_rpt")\
#    .saveAsTable("reporting.uwdfm_t_generic_pipeline_rpt")

# COMMAND ----------

#display(uwdfm_t_generic_pipeline_rpt_df)

# COMMAND ----------

#cogs_final_df.count()

# COMMAND ----------

# MAGIC %md ###Formulary Shift Strategy

# COMMAND ----------

# ### Only execute this cell when the first 'if' condition is satisfied

# if "uwdfm_t_formulary_shift_strategy_rpt" not in tblList or process_time_delta_df.count()!=0:

#   if  "uwdfm_t_formulary_shift_strategy_rpt" in tblList:
#     process_time_delta_df_fsr1=spark.sql("""
#     Select 
#     uw_req_id,req_gid,cpt_id
#     from processTime3
#     where 1=1
#     and concat(uw_req_id,req_gid,cpt_id) NOT IN
#     (Select
#     concat(uw_req_id,req_gid,cpt_id)
#     from process_time_initial)
#     and client_description is not null
#     """)
#   else:
#     process_time_delta_df_fsr1=spark.sql("""
#     Select 
#     uw_req_id,req_gid,cpt_id--,client_description
#     from processTime3
#     where client_description is not null
#     """)

#   ptd_df=process_time_delta_df_fsr1.createOrReplaceTempView("fsr_ptd")

#   fsr_final_df=spark.sql("""
# select
# '01_preshift' as module,
# cps.parent_request_id,
# cps.request_id as request_id,
# cps.task_id,
# prop_frmly_id,
# prop_ntwrk,
# 'preshift' as shift_type,
# case when (gpi IN ('50250065007220','50250065007240','50250065050310','50250065050320','50250065050340','50250065052024','50250065052030','50250065052070','50250065152040','50250065202001'
# ,'50250065202003','50250065202005','50250065202006','83101020102012','83101020102013','83101020102014','83101020102015','83101020102016','83101020102018','83101020102020'
# ,'83101020102050','96706812032900') or speclock = 'Y') and pmcy IN ('0360963', '0591796', '0722581', '1075224', '1153852','1158422', '1203417', '1466033', '1480019', '1492975','1715830', '2132924', '2228535', '2353542', '2374584','2429113', '2590215', '3137141', '3195268', '3195333','3431397', '3958898', '3994844', '4026325', '4429216','4439875', '5131228', '5922085', '1009287') then 'CVS Specialty' when dlvry_systm_cd ='R' THEN 'Retail' when dlvry_systm_cd ='M' THEN 'Mail' else 'Retail' END AS channel,
# genbrand,
# sum(claims) as claims,
# sum(dayssply) as days,
# sum(decqty) as qty,
# sum(currawp) as awp,
# sum(currwac) as wac
# FROM dfm.out_uwdfm_t_cc_claims_preshift cps
# JOIN fsr_ptd fptd on cps.parent_request_id=fptd.uw_req_id and cps.request_id=fptd.req_gid and cps.task_id=fptd.cpt_id
# GROUP BY 1,2,3,4,5,6,7,8,9

# UNION ALL

# select
# '02_formulary_shifted' as module,
# fs.parent_request_id,
# fs.req_gid as request_id,
# fs.task_id,
# prop_frmly_id,
# prop_ntwrk,
# case when origclaim IN ('P','Y','NY') THEN 'Non-Shifted' WHEN origclaim ='N' THEN 'Shifted' ELSE 'unknown' END AS shift_type,
# case when (gpi IN ('50250065007220','50250065007240','50250065050310','50250065050320','50250065050340','50250065052024','50250065052030','50250065052070','50250065152040','50250065202001'
# ,'50250065202003','50250065202005','50250065202006','83101020102012','83101020102013','83101020102014','83101020102015','83101020102016','83101020102018','83101020102020'
# ,'83101020102050','96706812032900') or speclock = 'Y') and pmcy IN ('0360963', '0591796', '0722581', '1075224', '1153852','1158422', '1203417', '1466033', '1480019', '1492975','1715830', '2132924', '2228535', '2353542', '2374584','2429113', '2590215', '3137141', '3195268', '3195333','3431397', '3958898', '3994844', '4026325', '4429216','4439875', '5131228', '5922085', '1009287') then 'CVS Specialty' when dlvry_systm_cd ='R' THEN 'Retail' when dlvry_systm_cd ='M' THEN 'Mail' else 'Retail' END AS channel,
# genbrand,
# sum(claims) as claims,
# sum(dayssply) as days,
# sum(decqty) as qty,
# sum(currawp) as awp,
# sum(currwac) as wac
# FROM dfm.out_uwdfm_t_formulary_shifted fs
# JOIN fsr_ptd fptd on fs.parent_request_id=fptd.uw_req_id and fs.req_gid=fptd.req_gid and fs.task_id=fptd.cpt_id
# GROUP BY 1,2,3,4,5,6,7,8,9

# UNION ALL

# SELECT
# '03_hg_shifted' as module,
# hgs.parent_request_id,
# hgs.req_gid as request_id,
# hgs.task_id,
# prop_frmly_id,
# prop_ntwrk,
# case when hg_origclaim IN ('P','Y') THEN 'Non-Shifted' when hg_origclaim='N' THEN 'Shifted' else 'unknown' END AS shift_type,
# case when (gpi IN ('50250065007220','50250065007240','50250065050310','50250065050320','50250065050340','50250065052024','50250065052030','50250065052070','50250065152040','50250065202001'
# ,'50250065202003','50250065202005','50250065202006','83101020102012','83101020102013','83101020102014','83101020102015','83101020102016','83101020102018','83101020102020'
# ,'83101020102050','96706812032900') or speclock = 'Y') and pmcy IN ('0360963', '0591796', '0722581', '1075224', '1153852','1158422', '1203417', '1466033', '1480019', '1492975','1715830', '2132924', '2228535', '2353542', '2374584','2429113', '2590215', '3137141', '3195268', '3195333','3431397', '3958898', '3994844', '4026325', '4429216','4439875', '5131228', '5922085', '1009287') then 'CVS Specialty' when dlvry_systm_cd ='R' THEN 'Retail' when dlvry_systm_cd ='M' THEN 'Mail' else 'Retail' END AS channel,
# genbrand,
# sum(claims) as claims,
# sum(dayssply) as days,
# sum(decqty) as qty,
# sum(currawp) as awp,
# sum(currwac) as wac
# FROM dfm.out_uwdfm_t_hg_shifted hgs
# JOIN fsr_ptd fptd on hgs.parent_request_id=fptd.uw_req_id and hgs.req_gid=fptd.req_gid and hgs.task_id=fptd.cpt_id
# GROUP BY 1,2,3,4,5,6,7,8,9


# UNION ALL

# SELECT
# '04_daw9_shifted' as module,
# d9s.parent_request_id,
# d9s.req_gid as request_id,
# d9s.task_id,
# prop_frmly_id,
# prop_ntwrk,
# case WHEN daw9_origclaim='P' THEN 'Non-Shifted' WHEN daw9_origclaim='N' THEN 'Shifted' ELSE 'unknown' END AS shift_type,
# case when (gpi IN ('50250065007220','50250065007240','50250065050310','50250065050320','50250065050340','50250065052024','50250065052030','50250065052070','50250065152040','50250065202001'
# ,'50250065202003','50250065202005','50250065202006','83101020102012','83101020102013','83101020102014','83101020102015','83101020102016','83101020102018','83101020102020'
# ,'83101020102050','96706812032900') or speclock = 'Y') and pmcy IN ('0360963', '0591796', '0722581', '1075224', '1153852','1158422', '1203417', '1466033', '1480019', '1492975','1715830', '2132924', '2228535', '2353542', '2374584','2429113', '2590215', '3137141', '3195268', '3195333','3431397', '3958898', '3994844', '4026325', '4429216','4439875', '5131228', '5922085', '1009287') then 'CVS Specialty' when dlvry_systm_cd ='R' THEN 'Retail' when dlvry_systm_cd ='M' THEN 'Mail' else 'Retail' END AS channel,
# genbrand,
# sum(claims) as claims,
# sum(dayssply) as days,
# sum(decqty) as qty,
# sum(currawp) as awp,
# sum(currwac) as wac
# FROM dfm.out_uwdfm_t_daw9_shifted d9s
# JOIN fsr_ptd fptd on d9s.parent_request_id=fptd.uw_req_id and d9s.req_gid=fptd.req_gid and d9s.task_id=fptd.cpt_id
# GROUP BY 1,2,3,4,5,6,7,8,9

# UNION ALL

# SELECT
# '05_pcsk9_shifted' as module,
# pcsk9.parent_request_id,
# pcsk9.req_gid as request_id,
# pcsk9.task_id,
# prop_frmly_id,
# prop_ntwrk,
# CASE WHEN mkt_shift_ind IN ('N') THEN 'Non-Shifted' WHEN mkt_shift_ind IN ('Y') THEN 'Shifted' ELSE 'unknown' END AS shift_type,
# case when (gpi IN ('50250065007220','50250065007240','50250065050310','50250065050320','50250065050340','50250065052024','50250065052030','50250065052070','50250065152040','50250065202001'
# ,'50250065202003','50250065202005','50250065202006','83101020102012','83101020102013','83101020102014','83101020102015','83101020102016','83101020102018','83101020102020'
# ,'83101020102050','96706812032900') or speclock = 'Y') and pmcy IN ('0360963', '0591796', '0722581', '1075224', '1153852','1158422', '1203417', '1466033', '1480019', '1492975','1715830', '2132924', '2228535', '2353542', '2374584','2429113', '2590215', '3137141', '3195268', '3195333','3431397', '3958898', '3994844', '4026325', '4429216','4439875', '5131228', '5922085', '1009287') then 'CVS Specialty' when dlvry_systm_cd ='R' THEN 'Retail' when dlvry_systm_cd ='M' THEN 'Mail' else 'Retail' END AS channel,
# genbrand,
# sum(claims) as claims,
# sum(dayssply) as days,
# sum(decqty) as qty,
# sum(CASE WHEN mkt_shift_ind ='Y' THEN (low_awpunit*decqty) ELSE currawp END) as awp,
# sum(CASE WHEN mkt_shift_ind ='Y' THEN (low_wacunit*decqty) ELSE currwac END) as wac
# FROM dfm.out_uwdfm_t_pcsk9_shifted pcsk9
# JOIN fsr_ptd fptd on pcsk9.parent_request_id=fptd.uw_req_id and pcsk9.req_gid=fptd.req_gid and pcsk9.task_id=fptd.cpt_id
# GROUP BY 1,2,3,4,5,6,7,8,9
# """).withColumn("current_timestamp",current_timestamp())
  
#   if  "uwdfm_t_formulary_shift_strategy_rpt" in tblList:
#     print('delta write')
#     if env1 ==1: ### Only do the merge/delete for test requests in DEV/QA
#     #if 1==1:
#       process_time_delta_df_fsr1.createOrReplaceTempView("fsr1")
#       fsr2_df = spark.read.format("delta").load("dbfs:/mnt/bronze/afp/dfm/reporting/uwdfm_t_formulary_shift_strategy_rpt").createOrReplaceTempView("fsr2")
#       CommonFSR_df=spark.sql("""
#       Select
#       S2.parent_request_id,
#       S2.request_id,
#       S2.task_id
#       from fsr2 S2 
#       join fsr1 S1 on S2.parent_request_id = S1.uw_req_id and S2.request_id = S1.req_gid and S2.task_id = S1.cpt_id
#       """).createOrReplaceTempView("CommonFSR")
#       query = "MERGE INTO delta.`dbfs:/mnt/bronze/afp/dfm/reporting/uwdfm_t_formulary_shift_strategy_rpt` AS d \
#               using (SELECT * FROM CommonFSR) AS k \
#               ON d.parent_request_id = k.parent_request_id \
#               AND d.request_id = k.request_id \
#               AND d.task_id = k.task_id \
#               WHEN MATCHED THEN DELETE"
#       sqlContext.sql(query)

#     fsr_final_df.write \
#    .format("delta") \
#    .partitionBy("parent_request_id","request_id","task_id") \
#    .mode("append") \
#    .option("mergeSchema", "true") \
#    .option("path",dfm_reporting_path+"uwdfm_t_formulary_shift_strategy_rpt")\
#    .saveAsTable("reporting.uwdfm_t_formulary_shift_strategy_rpt")


#   else:
#     print('first write')
#     fsr_final_df.write \
#    .format("delta") \
#    .partitionBy("parent_request_id","request_id","task_id") \
#    .mode("append") \
#    .option("mergeSchema", "true") \
#    .option("path",dfm_reporting_path + "uwdfm_t_formulary_shift_strategy_rpt")\
#    .saveAsTable("reporting.uwdfm_t_formulary_shift_strategy_rpt")

# COMMAND ----------

# MAGIC %md ###COGS Report

# COMMAND ----------

### Only execute this cell when the first 'if' condition is satisfied

if "uwdfm_t_cogs_rpt" not in tblList or process_time_delta_df.count()!=0:
  if  "uwdfm_t_cogs_rpt" in tblList:
    process_time_delta_df_cogs1=spark.sql("""
    Select 
    uw_req_id,req_gid,cpt_id
    from processTime3
    where 1=1
    and concat(uw_req_id,req_gid,cpt_id) NOT IN
    (Select
    concat(uw_req_id,req_gid,cpt_id)
    from process_time_initial)
    and client_description is not null
    """)
  else:
    process_time_delta_df_cogs1=spark.sql("""
    Select 
    uw_req_id,req_gid,cpt_id--,client_description
    from processTime3
    where client_description is not null
    """)

  ptd_df=process_time_delta_df_cogs1.createOrReplaceTempView("cogs_ptd")

  cogs_df=spark.sql("""select dc.*
  from dfm.out_uwdfm_t_output dc
  join cogs_ptd cptd on dc.PARENT_REQUEST_ID=cptd.uw_req_id and dc.REQUEST_ID=cptd.req_gid and dc.TASK_ID=cptd.cpt_id
  """)
  cogs_df.createOrReplaceTempView("cogs")

  cogs_final_df=spark.sql("""
  SELECT
'PBM' as Dataset_identifier,
T1.PARENT_REQUEST_ID as PARENT_REQUEST_ID,
T1.REQUEST_ID as REQ_GID,
T1.TASK_ID as TASK_ID,
T5.SUPER_CLIENT_ID as CLIENT_ID,
T5.client_description as CLIENT_NM,
T1.LOB as LOB,
T1.formulary_id as PROP_FRMLY_ID,
T3.FORMULARY_NAME as PROP_FRMLY_NAME,
T1.bg_pbmmon_mod as GENBRAND,
--((T1.prjctd_start_yr + T1.prjctd_yr) -1) as YEAR_cogs,
T1.prjctd_year as YEAR_cogs,
T1.prjctd_yr,
--cogs_network_type as cogs_network_type,
-- DE123665 Hot Fix Change
--concat(initcap(T7.type),' Formulary', ' // ', initcap(T1.proj_network_nm), ' Network') as cogs_network_type,
concat(initcap(T7.type),' Formulary', ' // ', initcap(T4.TYPE), ' Network') as cogs_network_type,
T1.FORMULARY_GUID,
T1.NETWORK_GUID,
T4.TYPE,
T1.PRIMARY_NTWRK as Cogs_network_primary,
T1.SECONDARY_NTWRK as Cogs_network_secondary,
T1.TERTIARY_NTWRK as Cogs_network_tertiary,
T1.proj_network_nm,
--case when (T1.lob_id ='STD' and T1.proj_network_nm = T1.PRIMARY_NTWRK) or (T1.lob_id !='STD' and T1.days_supply<45 and T1.proj_network_nm not like'%-%') then 'PRIMARY'
--when (T1.lob_id ='STD' and T1.proj_network_nm = T1.SECONDARY_NTWRK) or (T1.lob_id !='STD' and T1.days_supply>45 and T1.proj_network_nm not like'%-%') then 'SECONDARY'
--else 'TERTIARY' end as proj_network_type,
case when (T1.DAY_SPLY_QTY<45 and T1.proj_network_nm not like'%-%') then 'PRIMARY'
when (T1.DAY_SPLY_QTY>45 and T1.proj_network_nm not like'%-%') then 'SECONDARY'
else 'TERTIARY' end as proj_network_type,
concat(T1.PRIMARY_NTWRK,' // ',T1.SECONDARY_NTWRK,
case when (T1.TERTIARY_NTWRK is not null and T1.TERTIARY_NTWRK !='')  then ' // ' else '' end,
coalesce(T1.TERTIARY_NTWRK,'')) as prop_ntwrk,
CASE
    WHEN T1.is_capped = 1 THEN 'CAP' ELSE 'NONCAP'   
    END AS Contract_type,
T1.phmcy_chain_nm as Chname,
T1.phmcy_st as State,
T1.srx_shift_type as spec_type,
T5.super_client_id as super_client_id,
T1.opportunity_id as opportunity_id,
from_utc_timestamp(T5.inp_load_ts,'America/Chicago') as submit_date_cst,
--T6.Locked as splty_Speclock,
case when T1.spec_univ = 0 then 'N' else 'Y' end as splty_Speclock,
case when T6.LDD_flag is null then "N" else T6.LDD_flag end as LDD_flag,
Sum(T1.cogs_dispns_fee_trad_pbm) as Sum_trad_dispensing_fee,
Sum(T1.cogs_dispns_fee_trans_pbm) as Sum_trans_dispensing_fee,
Sum(T1.cogs_trans_pbm) as Sum_trans_ingredient_cost,
Sum(T1.cogs_trad_pbm) as Sum_trad_ingredient_cost,
Sum(T1.awp) as Sum_awp_cogs,
Sum(T1.claims_count) as Sum_claims_cogs
from cogs T1
LEFT JOIN (SELECT distinct FORMULARY_ID,FORMULARY_NAME FROM DFM.UWDFM_T_FORMULARY_LIST_MASTER) T3 ON T1.formulary_id = T3.FORMULARY_ID -- added distinct
LEFT JOIN (SELECT TYPE,TASK_ID, PARENT_REQUEST_ID, REQUEST_ID,PRIMARY, SECONDARY FROM DFM.UWDFM_T_REQ_INPUT_NETWORK) T4 ON T1.PARENT_REQUEST_ID = T4.PARENT_REQUEST_ID AND T1.REQUEST_ID = T4.REQUEST_ID AND T1.TASK_ID = T4.TASK_ID AND T1.PRIMARY_NTWRK = T4.PRIMARY AND T1.SECONDARY_NTWRK = T4.SECONDARY
-- DE123665 Hot Fix Change
INNER JOIN dfm.uwdfm_t_req_input_formulary T7 on T1.PARENT_REQUEST_ID = T7.parent_request_id and T1.REQUEST_ID = T7.request_id and T1.TASK_ID = T7.task_id and T1.FORMULARY_GUID = T7.formulary_guid
LEFT JOIN (SELECT NDC11,
           case 
when LDD_WACCESS ="N" and LDD_WOACCESS ="N" then "N" 
when LDD_WACCESS ="N" and LDD_WOACCESS ="Y" then "LDD_WOACCESS" 
when LDD_WACCESS ="Y" and LDD_WOACCESS ="N" then "LDD_WACCESS" end as LDD_flag 
from dfm.uwdfm_t_srx_master
group by 1,2
having count(*)=1) T6 on T1.ndc = T6.NDC11
INNER JOIN
(
-- DE127648 COGS Report Missing Client Name
SELECT CASE WHEN SUPER_CLIENT_ID IS NULL OR SUPER_CLIENT_ID='' THEN client_description ELSE SUPER_CLIENT_ID END AS SUPER_CLIENT_ID, OPPORTUNITY_ID, TASK_ID, PARENT_REQUEST_ID, REQUEST_ID,client_description, inp_load_ts FROM DFM.UWDFM_T_REQ_INPUT_PARAM
) T5 ON T1.PARENT_REQUEST_ID = T5.PARENT_REQUEST_ID AND T1.REQUEST_ID = T5.REQUEST_ID and T1.TASK_ID = T5.TASK_ID

WHERE T1.COB=0 AND T1.CMPND = 0 
AND T1.MC = 0 AND T1.SPEC_CMK_IND =0--IN ('CVSRET','other_retail')
AND T1.chnl_mail_mod = 0
AND T1.awp > 0 AND T1.claims_count > 0 AND T1.wac > 0

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,26,27,28,29,30,31

UNION ALL

SELECT
'MS' as Dataset_identifier,
T1.PARENT_REQUEST_ID as PARENT_REQUEST_ID,
T1.REQUEST_ID as REQ_GID,
T1.TASK_ID as TASK_ID,
T5.SUPER_CLIENT_ID as CLIENT_ID,
T5.client_description as CLIENT_NM,
T1.LOB as LOB,
T1.formulary_id as PROP_FRMLY_ID,
T3.FORMULARY_NAME as PROP_FRMLY_NAME,
T1.bg_mony_mod as GENBRAND,
--((T1.prjctd_start_yr + T1.prjctd_yr) -1) as YEAR_cogs,
T1.prjctd_year as YEAR_cogs,
T1.prjctd_yr,
--cogs_network_type as cogs_network_type,
-- DE123665 Hot Fix Change
--concat(initcap(T7.type),' Formulary', ' // ', initcap(T1.proj_network_nm), ' Network') as cogs_network_type,
concat(initcap(T7.type),' Formulary', ' // ', initcap(T4.TYPE), ' Network') as cogs_network_type,
T1.FORMULARY_GUID,
T1.NETWORK_GUID,
T4.TYPE,
T1.PRIMARY_NTWRK as Cogs_network_primary,
T1.SECONDARY_NTWRK as Cogs_network_secondary,
T1.TERTIARY_NTWRK as Cogs_network_tertiary,
T1.proj_network_nm,
--case when (T1.lob_id ='STD' and T1.proj_network_nm = T1.PRIMARY_NTWRK) or (T1.lob_id !='STD' and T1.days_supply<45 and T1.proj_network_nm not like'%-%') then 'PRIMARY'
--when (T1.lob_id ='STD' and T1.proj_network_nm = T1.SECONDARY_NTWRK) or (T1.lob_id !='STD' and T1.days_supply>45 and T1.proj_network_nm not like'%-%') then 'SECONDARY'
--else 'TERTIARY' end as proj_network_type,
case when (T1.DAY_SPLY_QTY<45 and T1.proj_network_nm not like'%-%') then 'PRIMARY'
when (T1.DAY_SPLY_QTY>45 and T1.proj_network_nm not like'%-%') then 'SECONDARY'
else 'TERTIARY' end as proj_network_type,
concat(T1.PRIMARY_NTWRK,' // ',T1.SECONDARY_NTWRK,
case when (T1.TERTIARY_NTWRK is not null and T1.TERTIARY_NTWRK !='') then ' // ' else '' end,
coalesce(T1.TERTIARY_NTWRK,'')) as prop_ntwrk,
CASE
    WHEN T1.is_capped = 1 THEN 'CAP' ELSE 'NONCAP'   
    END AS Contract_type,
T1.phmcy_chain_nm as Chname,
T1.phmcy_st as State,
T1.srx_shift_type as spec_type,
T5.super_client_id as super_client_id,
T1.opportunity_id as opportunity_id,
from_utc_timestamp(T5.inp_load_ts,'America/Chicago') as submit_date_cst,
--T6.Locked as splty_Speclock,
case when T1.spec_univ = 0 then 'N' else 'Y' end as splty_Speclock,
case when T6.LDD_flag is null then "N" else T6.LDD_flag end as LDD_flag,
Sum(T1.cogs_dispns_fee_trad_medi) as Sum_trad_dispensing_fee,
Sum(T1.cogs_dispns_fee_trans_medi) as Sum_trans_dispensing_fee,
Sum(T1.cogs_trans_medi) as Sum_trans_ingredient_cost,
Sum(T1.cogs_trad_medi) as Sum_trad_ingredient_cost,
Sum(T1.awp) as Sum_awp_cogs,
Sum(T1.claims_count) as Sum_claims_cogs
from cogs T1
LEFT JOIN (SELECT distinct FORMULARY_ID,FORMULARY_NAME FROM DFM.UWDFM_T_FORMULARY_LIST_MASTER) T3 ON T1.formulary_id = T3.FORMULARY_ID -- added distinct
LEFT JOIN (SELECT TYPE,TASK_ID, PARENT_REQUEST_ID, REQUEST_ID, PRIMARY, SECONDARY FROM DFM.UWDFM_T_REQ_INPUT_NETWORK) T4 ON T1.PARENT_REQUEST_ID = T4.PARENT_REQUEST_ID AND T1.REQUEST_ID = T4.REQUEST_ID AND T1.TASK_ID = T4.TASK_ID AND T1.PRIMARY_NTWRK = T4.PRIMARY AND T1.SECONDARY_NTWRK = T4.SECONDARY
-- DE123665 Hot Fix Change
INNER JOIN dfm.uwdfm_t_req_input_formulary T7 on T1.PARENT_REQUEST_ID = T7.parent_request_id and T1.REQUEST_ID = T7.request_id and T1.TASK_ID = T7.task_id and T1.FORMULARY_GUID = T7.formulary_guid
LEFT JOIN (SELECT NDC11,
           case 
when LDD_WACCESS ="N" and LDD_WOACCESS ="N" then "N" 
when LDD_WACCESS ="N" and LDD_WOACCESS ="Y" then "LDD_WOACCESS" 
when LDD_WACCESS ="Y" and LDD_WOACCESS ="N" then "LDD_WACCESS" end as LDD_flag 
from dfm.uwdfm_t_srx_master
group by 1,2
having count(*)=1) T6 on T1.ndc = T6.NDC11
INNER JOIN
(
-- DE127648 COGS Report Missing Client Name
SELECT CASE WHEN SUPER_CLIENT_ID IS NULL OR SUPER_CLIENT_ID='' THEN client_description ELSE SUPER_CLIENT_ID END AS SUPER_CLIENT_ID, OPPORTUNITY_ID, TASK_ID, PARENT_REQUEST_ID, REQUEST_ID,client_description, inp_load_ts FROM DFM.UWDFM_T_REQ_INPUT_PARAM
) T5 ON T1.PARENT_REQUEST_ID = T5.PARENT_REQUEST_ID AND T1.REQUEST_ID = T5.REQUEST_ID and T1.TASK_ID = T5.TASK_ID

WHERE T1.COB=0 AND T1.CMPND = 0 
AND T1.MC = 0 AND T1.SPEC_CMK_IND =0-- IN ('CVSRET','other_retail')
AND T1.chnl_mail_mod = 0
AND T1.awp > 0 AND T1.claims_count > 0 AND T1.wac > 0

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31
""").withColumn("current_timestamp",current_timestamp())

  if  "uwdfm_t_cogs_rpt" in tblList:
    print('delta write')
    if env1 ==1: ### Only do the merge/delete for test requests in DEV/QA
    #if 1==1:
      process_time_delta_df_cogs1.createOrReplaceTempView("cogs1")
      cogs2_df = spark.read.table("reporting.uwdfm_t_cogs_rpt").createOrReplaceTempView("cogs2")
      CommonCOGS_df=spark.sql("""
      Select
      S2.parent_request_id,
      S2.req_gid,
      S2.task_id
      from cogs2 S2 
      join cogs1 S1 on S2.parent_request_id = S1.uw_req_id and S2.req_gid = S1.req_gid and S2.task_id = S1.cpt_id
      """).createOrReplaceTempView("CommonCOGS")
      query = "MERGE INTO reporting.uwdfm_t_cogs_rpt` AS d \
              using (SELECT * FROM CommonCOGS) AS k \
              ON d.parent_request_id = k.parent_request_id \
              AND d.req_gid = k.req_gid \
              AND d.task_id = k.task_id \
              WHEN MATCHED THEN DELETE"
      spark.sql(query)

    cogs_final_df.write \
   .format("delta") \
   .partitionBy("PARENT_REQUEST_ID","REQ_GID","TASK_ID") \
   .mode("append") \
   .option("mergeSchema", "true") \
   .option("path",dfm_reporting_path+"uwdfm_t_cogs_rpt")\
   .saveAsTable("reporting.uwdfm_t_cogs_rpt")


  else:
    print('first write')
    cogs_final_df.write \
   .format("delta") \
   .partitionBy("PARENT_REQUEST_ID","REQ_GID","TASK_ID") \
   .mode("append") \
   .option("mergeSchema", "true") \
   .option("path",dfm_reporting_path + "uwdfm_t_cogs_rpt")\
   .saveAsTable("reporting.uwdfm_t_cogs_rpt")

# COMMAND ----------

# MAGIC %md ###COGS Model Report

# COMMAND ----------

### Only execute this cell when the first 'if' condition is satisfied

if "uwdfm_t_cogs_mdl_rpt" not in tblList or process_time_delta_df_cogs_mdl1.count()!=0:

  ptd_df=process_time_delta_df_cogs_mdl1.createOrReplaceTempView("cogs_mdl_ptd")

  cogs_mdl_df=spark.sql("""
  select * from
  (                      
  select
  cmu.data_request_id as m_data_request_id,
  cmu.scenario_id as m_scenario_id,
  cmu.business_type_id as m_business_type_id,
  cmu.brand_generic_definition as m_brand_generic_definition,
  cmu.pusedo_brand_generic_definition as m_pusedo_brand_generic_definition,
  cmu.secondary_network as m_secondary_network,
  cmu.pricing_type as m_pricing_type,
  cr.Dataset_identifier,
  cr.PARENT_REQUEST_ID,
  cr.REQ_GID,
  cr.TASK_ID,
  cr.CLIENT_ID,
  cr.CLIENT_NM,
  cr.LOB,
  cr.PROP_FRMLY_ID,
  --cr.PROP_FRMLY_NAME,
  case when cr.LOB='WRAP' then cmu.formulary else cr.PROP_FRMLY_NAME end as PROP_FRMLY_NAME,
  cr.GENBRAND,
  cr.YEAR_cogs,
  cr.prjctd_yr,
  cr.cogs_network_type,
  cr.Cogs_network_primary,
  cr.Cogs_network_secondary,
  cr.Cogs_network_tertiary,
  case when cr.proj_network_type ='PRIMARY' then cmu.PRIMARY_NETWORK
       when cr.proj_network_type ='SECONDARY' then cmu.SECONDARY_NETWORK
       else cmu.tertiary_network_nm end as proj_network_nm,
  cr.proj_network_type,
  cr.prop_ntwrk,
  cr.type as model_type,
  cr.Contract_type,
  cr.Chname,
  cr.State,
  cr.spec_type,
  cr.super_client_id,
  cr.opportunity_id,
  cr.submit_date_cst,
  cr.splty_Speclock,
  cr.LDD_flag as LDD_DFM_flag,
  cmu.LDD_OPTION as LDD_flag,
  case when cmu.LDD_OPTION ='LDD w/ Access Only' and cr.LDD_flag = 'N' then 'Include'
       when cmu.LDD_OPTION ='LDD w/ Access Only' and cr.LDD_flag = 'LDD_WACCESS' then 'Exclude'
       when cmu.LDD_OPTION ='LDD w/ Access Only' and cr.LDD_flag = 'LDD_WOACCESS' then 'Include'
       when cmu.LDD_OPTION ='LDD w/o Access Only' and cr.LDD_flag = 'N' then 'Include'
       when cmu.LDD_OPTION ='LDD w/o Access Only' and cr.LDD_flag = 'LDD_WACCESS' then 'Include'
       when cmu.LDD_OPTION ='LDD w/o Access Only' and cr.LDD_flag = 'LDD_WOACCESS' then 'Exclude'
       when cmu.LDD_OPTION ='Broad (w/ Access + w/o Access)' and cr.LDD_flag = 'N' then 'Include'
       when cmu.LDD_OPTION ='Broad (w/ Access + w/o Access)' and cr.LDD_flag = 'LDD_WACCESS' then 'Exclude'
       when cmu.LDD_OPTION ='Broad (w/ Access + w/o Access)' and cr.LDD_flag = 'LDD_WOACCESS' then 'Exclude'
       when cmu.LDD_OPTION ='All_Included' then 'Include'
       end as LDD_tbl_flag,
  cr.Sum_trad_dispensing_fee,
  cr.Sum_trans_dispensing_fee,
  cr.Sum_trans_ingredient_cost,
  cr.Sum_trad_ingredient_cost,
  cr.Sum_awp_cogs,
  cr.Sum_claims_cogs,
  cr.current_timestamp
  from reporting.uwdfm_t_cogs_rpt cr
  left join
  (
  select distinct
  mdl.opportunity_id,mdl.data_request_id,mdl.task_id,mdl.business_type,mdl.submit_date_cst,mdl.scenario_id,mdl.business_type_id,
  mdl.brand_generic_definition,mdl.pusedo_brand_generic_definition,
  mdl.formulary,mdl.formulary_guid,mdl.network_guid,mdl.type,mdl.LDD_flag,mdl.pricing_type,mdl.LDD_OPTION,mdl.PRIMARY_NETWORK,mdl.secondary_network,
  stack(5, mdl.NETWORK_3, mdl.NETWORK_4, mdl.NETWORK_5, mdl.NETWORK_6,mdl.NETWORK_7)as tertiary_network_nm
  from reporting.ce_model_utility_rpt mdl
  join cogs_mdl_ptd mdlp on mdl.data_request_id=mdlp.data_request_id and mdl.scenario_id=mdlp.scenario_id and mdl.business_type_id=mdlp.business_type_id
  --where scenario_id='C524EAA604A847DDA73CD9C229FFD3D5'
  ) cmu on cr.opportunity_id=cmu.opportunity_id 
  and cr.PARENT_REQUEST_ID=cmu.data_request_id 
  and cr.TASK_ID=cmu.task_id 
  and cr.lob=cmu.business_type 
  and cr.submit_date_cst=cmu.submit_date_cst 
  and cr.Dataset_identifier=cmu.pusedo_brand_generic_definition 
  --and cr.Cogs_network_primary=upper(cmu.primary_network) 
  --and cr.Cogs_network_secondary=upper(cmu.secondary_network)
  and (case when cr.LOB='WRAP' then cmu.formulary else cr.PROP_FRMLY_NAME end)=cmu.formulary
  and cr.FORMULARY_GUID=cmu.formulary_guid
  and cr.NETWORK_GUID=cmu.network_guid
  and upper(cr.type)=upper(cmu.type)
  --and cr.LDD_flag = cmu.LDD_flag
  where 1=1
  --and cr.CLIENT_NM in ('ALLSTATE','MELROSE GKN AEROSPACE','GLAXOSMITHKLINE') 
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45
  ) where  m_data_request_id is not null                   
  """)

  if "uwdfm_t_cogs_mdl_rpt" in tblList:
    print('delta write')
    cogs_mdl_df.write \
   .format("delta") \
   .partitionBy("PARENT_REQUEST_ID","REQ_GID","TASK_ID") \
   .mode("append") \
   .option("mergeSchema", "true") \
   .option("path",dfm_reporting_path + "uwdfm_t_cogs_mdl_rpt")\
   .saveAsTable("reporting.uwdfm_t_cogs_mdl_rpt")

  else:
    print('first write')
    cogs_mdl_df.write \
   .format("delta") \
   .partitionBy("PARENT_REQUEST_ID","REQ_GID","TASK_ID") \
   .mode("append") \
   .option("mergeSchema", "true") \
   .option("path",dfm_reporting_path + "uwdfm_t_cogs_mdl_rpt")\
   .saveAsTable("reporting.uwdfm_t_cogs_mdl_rpt")

# COMMAND ----------

# MAGIC %md ###GPI Report

# COMMAND ----------

### Only execute this cell when the first 'if' condition is satisfied

if "uwdfm_t_gpi_rpt" not in tblList or process_time_delta_df.count()!=0:
  if  "uwdfm_t_gpi_rpt" in tblList:
    process_time_delta_df_gpi1=spark.sql("""
    Select 
    uw_req_id,req_gid,cpt_id
    from processTime3
    where 1=1
    and concat(uw_req_id,req_gid,cpt_id) NOT IN
    (Select
    concat(uw_req_id,req_gid,cpt_id)
    from process_time_initial)
    and client_description is not null
    """)
  else:
    process_time_delta_df_gpi1=spark.sql("""
    Select 
    uw_req_id,req_gid,cpt_id--,client_description
    from processTime3
    where client_description is not null
    """)

  ptd_df=process_time_delta_df_gpi1.createOrReplaceTempView("gpi_ptd")

  gpi_df=spark.sql("""select o.*
  from dfm.out_uwdfm_t_output o
  join gpi_ptd cptd on o.PARENT_REQUEST_ID=cptd.uw_req_id and o.REQUEST_ID=cptd.req_gid and o.TASK_ID=cptd.cpt_id
  where 1=1
  --and o.srx_shift_type in ('NON SPEC SHIFT', 'NO SHIFT') -- 'EXCLUSIVE TO OPEN', 'OPEN TO EXCLUSIVE'
  and o.claims_count > 0
  and o.wac>0 
  and o.awp > 0
  and o.chnl_mail_mod = 0
  and o.chnl_ret_mod = 1
  and o.chnl_spec = 0
  """)
  gpi_df.createOrReplaceTempView("gpi_out")

  gpi_final_df=spark.sql("""
  select
  'MS' as Dataset_identifier,
  -- o.chnl_mail_mod,
  -- o.chnl_ret_mod,
  -- o.chnl_ret_spec,
  -- o.chnl_spec,
  o.PARENT_REQUEST_ID as PARENT_REQUEST_ID,
  o.REQUEST_ID as REQ_GID,
  o.TASK_ID as TASK_ID,
  coalesce(rip.super_client_id,rip.client_description) as CLIENT_ID,
  rip.client_description as CLIENT_NM,
  o.LOB as LOB,
  o.PRIMARY_NTWRK,
  o.SECONDARY_NTWRK,
  o.TERTIARY_NTWRK,
  o.formulary_id as PROP_FRMLY_ID,
  T3.FORMULARY_NAME as PROP_FRMLY_NAME,
  T4.TYPE,
  o.proj_network_nm,
  --case when o.proj_network_nm = o.PRIMARY_NTWRK then 'PRIMARY'
   --  when o.proj_network_nm = o.SECONDARY_NTWRK then 'SECONDARY'
   --  else 'TERTIARY' end as proj_network_type,
  --case when (o.lob_id ='STD' and o.proj_network_nm = o.PRIMARY_NTWRK) or (o.lob_id !='STD' and o.days_supply<45 and o.proj_network_nm not like'%-%') then 'PRIMARY'
  --when (o.lob_id ='STD' and o.proj_network_nm = o.SECONDARY_NTWRK) or (o.lob_id !='STD' and o.days_supply>45 and o.proj_network_nm not like'%-%') then 'SECONDARY'
  --else 'TERTIARY' end as proj_network_type,
  case when (o.DAY_SPLY_QTY<45 and o.proj_network_nm not like'%-%') then 'PRIMARY'
  when (o.DAY_SPLY_QTY>45 and o.proj_network_nm not like'%-%') then 'SECONDARY'
  else 'TERTIARY' end as proj_network_type,
  --o.contract_type,
  rip.super_client_id,
  rip.opportunity_id,
  from_utc_timestamp(rip.inp_load_ts,'America/Chicago') as submit_date_cst,
  o.SPEC_DRG as spec_drug,
  o. SPEC_CMK_IND as spec_cmk,
  o.spec_lock_ees,
  o.spec_lock_no_ees,
  o.spec_univ,
  case when srxm.ldd_flag is null then "N" else srxm.ldd_flag end as LDD_flag,
  o.formulary_nm,
  o.srx_shift_type,
  o.phmcy_st as State,
  o.phmcy_chain_nm as Chname,
  --date(dateadd(year,-1,DATEADD(year, o.prjctd_yr, rip.project_start_year))) as forecast_start_date,
  date(dateadd(year,-1,DATEADD(year, o.prjctd_yr, rip.project_start_year))) as forecast_start_date,
  --((o.prjctd_start_yr + o.prjctd_yr) -1) forecast_year,
  o.prjctd_year as forecast_year,
  o.prjctd_yr as projected_year_number,
  o.bg_mony_mod,
  d.gpi_class,
  d.gpi_categ as Drug_Categ,
  o.MODEL_DRUG as Drug_Name,
  d.gpi_group,
  initcap(f.non_specialty) as non_specialty,
  initcap(f.specialty) as specialty,
  initcap(n.primary) as primary,
  initcap(n.secondary) as secondary,
  o.FORMULARY_GUID,
  o.NETWORK_GUID,
  sum(o.cogs_trad_medi) trad_ms_ingredient_cost,
  sum(o.cogs_trans_medi) trans_ms_ingredient_cost,
  sum(o.cogs_dispns_fee_trad_medi) trad_ms_dispns_fee,
  sum(o.cogs_dispns_fee_trans_medi) trans_ms_dispns_fee,
  sum(o.awp) Total_AWP,
  sum(o.claims_count) claims_count
from
  gpi_out o
  left join (
    select
      ndc11,
      gpi_class,
      gpi_categ,
      gpi_group
    from
      dfm.uwdfm_t_drug_master QUALIFY count(*) over(partition by ndc11) = 1
  ) d on d.ndc11 = o.ndc
  left join dfm.uwdfm_t_req_input_network n on n.parent_request_id = o.PARENT_REQUEST_ID
  and n.request_id = o.REQUEST_ID
  and n.task_id = o.TASK_ID
  and o.NETWORK_GUID = n.network_guid
  left join dfm.uwdfm_t_req_input_formulary f on f.parent_request_id = o.PARENT_REQUEST_ID
  and f.request_id = o.REQUEST_ID
  and f.task_id = o.TASK_ID
  and f.formulary_guid = o.FORMULARY_GUID
  join
(
select client_description ,super_client_id, opportunity_id, task_id, parent_request_id, request_id, inp_load_ts,project_start_year from dfm.uwdfm_t_req_input_param
where 1=1
--and client_description in ('AIMBRIDGE HOSPITALITY','JBSUSA')
) rip on o.TASK_ID = rip.task_id and o.PARENT_REQUEST_ID = rip.parent_request_id and o.REQUEST_ID = rip.request_id
left join (select ndc11,
           case 
when ldd_waccess ="n" and ldd_woaccess ="n" then "n" 
when ldd_waccess ="n" and ldd_woaccess ="y" then "ldd_woaccess" 
when ldd_waccess ="y" and ldd_woaccess ="n" then "ldd_waccess" end as ldd_flag 
from dfm.uwdfm_t_srx_master
group by 1,2
having count(*)=1) srxm on o.ndc = srxm.ndc11
left join (select distinct FORMULARY_ID,FORMULARY_NAME FROM DFM.UWDFM_T_FORMULARY_LIST_MASTER) T3 ON o.formulary_id = T3.FORMULARY_ID
left join (select TYPE,TASK_ID, PARENT_REQUEST_ID, REQUEST_ID, PRIMARY, SECONDARY FROM DFM.UWDFM_T_REQ_INPUT_NETWORK) T4 ON o.PARENT_REQUEST_ID = T4.PARENT_REQUEST_ID AND o.REQUEST_ID = T4.REQUEST_ID AND o.TASK_ID = T4.TASK_ID AND o.PRIMARY_NTWRK = T4.PRIMARY AND o.SECONDARY_NTWRK = T4.SECONDARY
where
  date(from_utc_timestamp(rip.inp_load_ts,'America/Chicago')) >='2023-01-01' 
Group by
1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42--,43
UNION ALL
select
  'PBM' as Dataset_identifier,
  -- o.chnl_mail_mod,
  -- o.chnl_ret_mod,
  -- o.chnl_ret_spec,
  -- o.chnl_spec,
  o.PARENT_REQUEST_ID as PARENT_REQUEST_ID,
  o.REQUEST_ID as REQ_GID,
  o.TASK_ID as TASK_ID,
  coalesce(rip.super_client_id,rip.client_description) as CLIENT_ID,
  rip.client_description as CLIENT_NM,
  o.LOB as LOB,
  o.PRIMARY_NTWRK,
  o.SECONDARY_NTWRK,
  o.TERTIARY_NTWRK,
  o.formulary_id as PROP_FRMLY_ID,
  T3.FORMULARY_NAME as PROP_FRMLY_NAME,
  T4.TYPE,
  o.proj_network_nm,
  --case when o.proj_network_nm = o.PRIMARY_NTWRK then 'PRIMARY'
   --  when o.proj_network_nm = o.SECONDARY_NTWRK then 'SECONDARY'
    -- else 'TERTIARY' end as proj_network_type,
  --case when (o.lob_id ='STD' and o.proj_network_nm = o.PRIMARY_NTWRK) or (o.lob_id !='STD' and o.days_supply<45 and o.proj_network_nm not like'%-%') then 'PRIMARY'
  --when (o.lob_id ='STD' and o.proj_network_nm = o.SECONDARY_NTWRK) or (o.lob_id !='STD' and o.days_supply>45 and o.proj_network_nm not like'%-%') then 'SECONDARY'
  --else 'TERTIARY' end as proj_network_type,
  case when (o.DAY_SPLY_QTY<45 and o.proj_network_nm not like'%-%') then 'PRIMARY'
  when (o.DAY_SPLY_QTY>45 and o.proj_network_nm not like'%-%') then 'SECONDARY'
  else 'TERTIARY' end as proj_network_type,
  --o.contract_type,
  rip.super_client_id,
  rip.opportunity_id,
  from_utc_timestamp(rip.inp_load_ts,'America/Chicago') as submit_date_cst,
  o.SPEC_DRG as spec_drug,
  o. SPEC_CMK_IND as spec_cmk,
  o.spec_lock_ees,
  o.spec_lock_no_ees,
  o.spec_univ,
  case when srxm.ldd_flag is null then "N" else srxm.ldd_flag end as LDD_flag,
  o.formulary_nm,
  o.srx_shift_type,
  o.phmcy_st as State,
  o.phmcy_chain_nm as Chname,
  date(dateadd(year,-1,DATEADD(year, o.prjctd_yr, rip.project_start_year))) as forecast_start_date,
  --((o.prjctd_start_yr + o.prjctd_yr) -1) forecast_year,
  o.prjctd_year as forecast_year,
  o.prjctd_yr as projected_year_number,
  o.bg_pbmmon_mod,
  d.gpi_class,
  d.gpi_categ as Drug_Categ,
  o.MODEL_DRUG as Drug_Name,
  d.gpi_group,
  initcap(f.non_specialty) as non_specialty,
  initcap(f.specialty) as specialty,
  initcap(n.primary) as primary,
  initcap(n.secondary) as secondary,
  o.FORMULARY_GUID,
  o.NETWORK_GUID,
  sum(o.cogs_trad_pbm) trad_pbm_ingredient_cost,
  sum(o.cogs_trans_pbm) trans_pbm_ingredient_cost,
  sum(o.cogs_dispns_fee_trad_pbm) trad_pbm_dispns_fee,
  sum(o.cogs_dispns_fee_trans_pbm) trans_pbm_dispns_fee,
  sum(o.awp) Total_AWP,
  sum(o.claims_count) claims_count
from
  gpi_out o
  left join (
    select
      ndc11,
      gpi_class,
      gpi_categ,
      gpi_group
    from
      dfm.uwdfm_t_drug_master QUALIFY count(*) over(partition by ndc11) = 1
  ) d on d.ndc11 = o.ndc
  left join dfm.uwdfm_t_req_input_network n on n.parent_request_id = o.PARENT_REQUEST_ID
  and n.request_id = o.REQUEST_ID
  and n.task_id = o.TASK_ID
  and o.NETWORK_GUID = n.network_guid
  left join dfm.uwdfm_t_req_input_formulary f on f.parent_request_id = o.PARENT_REQUEST_ID
  and f.request_id = o.REQUEST_ID
  and f.task_id = o.TASK_ID
  and f.formulary_guid = o.FORMULARY_GUID
  join
(
select client_description ,super_client_id, opportunity_id, task_id, parent_request_id, request_id, inp_load_ts,project_start_year from dfm.uwdfm_t_req_input_param
where 1=1
--and client_description in ('AIMBRIDGE HOSPITALITY','JBSUSA')
) rip on o.TASK_ID = rip.task_id and o.PARENT_REQUEST_ID = rip.parent_request_id and o.REQUEST_ID = rip.request_id
left join (select ndc11,
           case 
when ldd_waccess ="n" and ldd_woaccess ="n" then "n" 
when ldd_waccess ="n" and ldd_woaccess ="y" then "ldd_woaccess" 
when ldd_waccess ="y" and ldd_woaccess ="n" then "ldd_waccess" end as ldd_flag 
from dfm.uwdfm_t_srx_master
group by 1,2
having count(*)=1) srxm on o.ndc = srxm.ndc11
left join (select distinct FORMULARY_ID,FORMULARY_NAME FROM DFM.UWDFM_T_FORMULARY_LIST_MASTER) T3 ON o.formulary_id = T3.FORMULARY_ID
left join (select TYPE,TASK_ID, PARENT_REQUEST_ID, REQUEST_ID, PRIMARY, SECONDARY FROM DFM.UWDFM_T_REQ_INPUT_NETWORK) T4 ON o.PARENT_REQUEST_ID = T4.PARENT_REQUEST_ID AND o.REQUEST_ID = T4.REQUEST_ID AND o.TASK_ID = T4.TASK_ID AND o.PRIMARY_NTWRK = T4.PRIMARY AND o.SECONDARY_NTWRK = T4.SECONDARY
where
  date(from_utc_timestamp(rip.inp_load_ts,'America/Chicago')) >='2023-01-01' 
Group by
1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42--,43
  """).withColumn("current_timestamp",current_timestamp())

  if  "uwdfm_t_gpi_rpt" in tblList:
    print('delta write')
    if env1 ==1: ### Only do the merge/delete for test requests in DEV/QA
    #if 1==1:
      process_time_delta_df_gpi1.createOrReplaceTempView("gpi1")
      cogs2_df = spark.read.table("reporting.uwdfm_t_gpi_rpt").createOrReplaceTempView("gpi2")
      Commongpi_df=spark.sql("""
      Select
      S2.parent_request_id,
      S2.req_gid,
      S2.task_id
      from gpi2 S2 
      join gpi1 S1 on S2.parent_request_id = S1.uw_req_id and S2.req_gid = S1.req_gid and S2.task_id = S1.cpt_id
      """).createOrReplaceTempView("CommonGPI")
      query = "MERGE INTO reporting.uwdfm_t_gpi_rpt AS d \
              using (SELECT * FROM CommonGPI) AS k \
              ON d.parent_request_id = k.parent_request_id \
              AND d.req_gid = k.req_gid \
              AND d.task_id = k.task_id \
              WHEN MATCHED THEN DELETE"
      spark.sql(query)

    gpi_final_df.write \
   .format("delta") \
   .partitionBy("opportunity_id","PARENT_REQUEST_ID","REQ_GID","TASK_ID") \
   .mode("append") \
   .option("mergeSchema", "true") \
   .option("path",dfm_reporting_path + "uwdfm_t_gpi_rpt")\
   .saveAsTable("reporting.uwdfm_t_gpi_rpt")


  else:
    print('first write')
    gpi_final_df.write \
   .format("delta") \
   .partitionBy("opportunity_id","PARENT_REQUEST_ID","REQ_GID","TASK_ID") \
   .mode("append") \
   .option("mergeSchema", "true") \
   .option("path",dfm_reporting_path + "uwdfm_t_gpi_rpt")\
   .saveAsTable("reporting.uwdfm_t_gpi_rpt")

# COMMAND ----------

# MAGIC %md ###GPI Model Report

# COMMAND ----------

### Only execute this cell when the first 'if' condition is satisfied

if "uwdfm_t_gpi_mdl_rpt" not in tblList or process_time_delta_df_gpi_mdl1.count()!=0:

  ptd_df=process_time_delta_df_gpi_mdl1.createOrReplaceTempView("gpi_mdl_ptd")

  gpi_mdl_df=spark.sql("""
  select * from
  (                      
  select
  cmu.data_request_id as m_data_request_id,
  cmu.scenario_id as m_scenario_id,
  cmu.business_type_id as m_business_type_id,
  cmu.brand_generic_definition as m_brand_generic_definition,
  cmu.pusedo_brand_generic_definition as m_pusedo_brand_generic_definition,
  cmu.secondary_network as m_secondary_network,
  cmu.pricing_type as m_pricing_type,
  gr.Dataset_identifier,
  gr.PARENT_REQUEST_ID,
  gr.REQ_GID,
  gr.TASK_ID,
  gr.CLIENT_ID,
  gr.CLIENT_NM,
  gr.LOB,
  gr.PRIMARY_NTWRK,
  gr.SECONDARY_NTWRK,
  gr.TERTIARY_NTWRK,
  gr.PROP_FRMLY_ID,
  case when gr.LOB='WRAP' then cmu.formulary else gr.PROP_FRMLY_NAME end as PROP_FRMLY_NAME,
  gr.TYPE as model_type,
  case when gr.proj_network_type ='PRIMARY' then cmu.PRIMARY_NETWORK
       when gr.proj_network_type ='SECONDARY' then cmu.SECONDARY_NETWORK
       else cmu.tertiary_network_nm end as proj_network_nm,
  gr.proj_network_type,
  --gr.contract_type,
  gr.super_client_id,
  gr.opportunity_id,
  gr.submit_date_cst,
  gr.spec_drug,
  gr.spec_cmk,
  gr.spec_lock_ees,
  gr.spec_lock_no_ees,
  gr.spec_univ,
  gr.LDD_flag as LDD_DFM_flag,
  cmu.LDD_OPTION as LDD_flag,
  case when cmu.LDD_OPTION ='LDD w/ Access Only' and gr.LDD_flag = 'n' then 'Include'
       when cmu.LDD_OPTION ='LDD w/ Access Only' and gr.LDD_flag = 'ldd_waccess' then 'Exclude'
       when cmu.LDD_OPTION ='LDD w/ Access Only' and gr.LDD_flag = 'ldd_woccess' then 'Include'
       when cmu.LDD_OPTION ='LDD w/o Access Only' and gr.LDD_flag = 'n' then 'Include'
       when cmu.LDD_OPTION ='LDD w/o Access Only' and gr.LDD_flag = 'ldd_waccess' then 'Include'
       when cmu.LDD_OPTION ='LDD w/o Access Only' and gr.LDD_flag = 'ldd_woccess' then 'Exclude'
       when cmu.LDD_OPTION ='Broad (w/ Access + w/o Access)' and gr.LDD_flag = 'n' then 'Include'
       when cmu.LDD_OPTION ='Broad (w/ Access + w/o Access)' and gr.LDD_flag = 'ldd_waccess' then 'Exclude'
       when cmu.LDD_OPTION ='Broad (w/ Access + w/o Access)' and gr.LDD_flag = 'ldd_woccess' then 'Exclude'
       when cmu.LDD_OPTION ='All_Included' then 'Include'
       end as LDD_tbl_flag,
  gr.formulary_nm,
  gr.srx_shift_type,
  gr.State,
  gr.Chname,
  gr.forecast_start_date,
  gr.forecast_year,
  gr.projected_year_number,
  gr.bg_mony_mod,
  gr.gpi_class,
  gr.Drug_Categ,
  case when gr.Drug_Name='' then 'UNKNOWN' else gr.Drug_Name end as Drug_Name,
  gr.gpi_group,
  gr.non_specialty,
  gr.specialty,
  gr.primary,
  gr.secondary,
  gr.trad_ms_ingredient_cost,
  gr.trans_ms_ingredient_cost,
  gr.trad_ms_dispns_fee,
  gr.trans_ms_dispns_fee,
  gr.Total_AWP,
  gr.claims_count,
  gr.current_timestamp
  from reporting.uwdfm_t_gpi_rpt gr
  left join
  (
  select distinct
  mdl.opportunity_id,mdl.data_request_id,mdl.task_id,mdl.business_type,mdl.submit_date_cst,mdl.scenario_id,mdl.business_type_id,
  mdl.brand_generic_definition,mdl.pusedo_brand_generic_definition,
  mdl.formulary,mdl.formulary_guid,mdl.network_guid,mdl.type,mdl.LDD_flag,mdl.pricing_type,mdl.LDD_OPTION,mdl.PRIMARY_NETWORK,mdl.secondary_network,
  stack(5, mdl.NETWORK_3, mdl.NETWORK_4, mdl.NETWORK_5, mdl.NETWORK_6,mdl.NETWORK_7)as tertiary_network_nm
  from reporting.ce_model_utility_rpt mdl
  join gpi_mdl_ptd mdlp on mdl.data_request_id=mdlp.data_request_id and mdl.scenario_id=mdlp.scenario_id and mdl.business_type_id=mdlp.business_type_id
  --where scenario_id='C524EAA604A847DDA73CD9C229FFD3D5'
  ) cmu on gr.opportunity_id=cmu.opportunity_id 
  and gr.PARENT_REQUEST_ID=cmu.data_request_id 
  and gr.TASK_ID=cmu.task_id 
  and gr.lob=cmu.business_type 
  and gr.submit_date_cst=cmu.submit_date_cst 
  and gr.Dataset_identifier=cmu.pusedo_brand_generic_definition 
  and (case when gr.LOB='WRAP' then cmu.formulary else gr.PROP_FRMLY_NAME end)=cmu.formulary
  and upper(gr.type)=upper(cmu.type)
  and gr.FORMULARY_GUID=cmu.formulary_guid
  and gr.NETWORK_GUID=cmu.network_guid
  --and gr.LDD_flag = cmu.LDD_flag
  where 1=1
  --and cr.CLIENT_NM in ('ALLSTATE','MELROSE GKN AEROSPACE','GLAXOSMITHKLINE') 
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56--,57
  ) where  m_data_request_id is not null                   
  """)
  gpi_mdl_df.createOrReplaceTempView("gpi_mdl")

  if "uwdfm_t_gpi_mdl_rpt" in tblList:
    print('delta write')
    gpi_mdl_df.write \
   .format("delta") \
   .partitionBy("PARENT_REQUEST_ID","REQ_GID","TASK_ID") \
   .mode("append") \
   .option("mergeSchema", "true") \
   .option("path",dfm_reporting_path + "uwdfm_t_gpi_mdl_rpt")\
   .saveAsTable("reporting.uwdfm_t_gpi_mdl_rpt")

  else:
    print('first write')
    gpi_mdl_df.write \
   .format("delta") \
   .partitionBy("PARENT_REQUEST_ID","REQ_GID","TASK_ID") \
   .mode("append") \
   .option("mergeSchema", "true") \
   .option("path",dfm_reporting_path + "uwdfm_t_gpi_mdl_rpt")\
   .saveAsTable("reporting.uwdfm_t_gpi_mdl_rpt")

# COMMAND ----------

# MAGIC %md ###Request Turnaround Time

# COMMAND ----------

### Only execute this cell when the first 'if' condition is satisfied

if "uwdfm_t_req_input_param_rpt" not in tblList or "uwdfm_t_process_time" not in tblList or process_time_delta_df.count()!=0:
  
  queryInputParam0 = "Drop Table If Exists reporting.uwdfm_t_req_input_param_rpt"
  spark.sql(queryInputParam0)

  if DeltaTable.isDeltaTable(spark, dfm_reporting_path + "uwdfm_t_req_input_param_rpt") or DeltaTable.isDeltaTable(spark, dfm_reporting_path + "uwdfm_t_process_time"):
     queryInputParam2 = "DELETE FROM reporting.uwdfm_t_req_input_param_rpt"
     spark.sql(queryInputParam2)
     dbutils.fs.rm(dfm_reporting_path + "uwdfm_t_req_input_param_rpt/", True)

  queryInputParam2_df =spark.sql("""
             SELECT p1.*
             ,from_utc_timestamp(p1.inp_load_ts, 'America/Chicago') as inp_load_ts_cst
             FROM dfm.uwdfm_t_req_input_param p1
             join processTime3 p2 on p1.parent_request_id=p2.uw_req_id and p1.request_id=p2.req_gid and p1.task_id=p2.cpt_id
             where p2.status="COMPLETE"
             order by p1.inp_load_ts desc""")
  
  queryInputParam2_df.write \
   .format("delta") \
   .mode("append") \
   .option("mergeSchema", "true") \
   .option("path",dfm_reporting_path + "uwdfm_t_req_input_param_rpt")\
   .saveAsTable("reporting.uwdfm_t_req_input_param_rpt")

# COMMAND ----------

# MAGIC %md ###Input Params

# COMMAND ----------

### Only execute this cell when the first 'if' condition is satisfied
if "uwdfm_t_process_time" not in tblList or process_time_delta_df.count()!=0:
  query0 = "Drop Table If Exists reporting.uwdfm_t_process_time"
  spark.sql(query0)

  if DeltaTable.isDeltaTable(spark, dfm_reporting_path + "uwdfm_t_process_time"):
    query1 = "DELETE FROM reporting.uwdfm_t_process_time"
    spark.sql(query1)
    dbutils.fs.rm(dfm_reporting_path + "uwdfm_t_process_time/", True)
    
  query2_df =spark.sql("""
             SELECT *
             FROM processTime3
             where client_description is not null
             and status="COMPLETE" 
             """)

  query2_df.write \
   .format("delta") \
   .mode("append") \
   .option("mergeSchema", "true") \
   .option("path",dfm_reporting_path + "uwdfm_t_process_time")\
   .saveAsTable("reporting.uwdfm_t_process_time")

# COMMAND ----------

# MAGIC %md ###Dates Dimension

# COMMAND ----------

### Only execute this cell when the first 'if' condition is satisfied
if "uwdfm_t_dates_dim_rpt" not in tblList:
  beginDate = '2000-01-01'
  endDate = '2099-12-31'


  range_df=spark.sql(f"select explode(sequence(to_date('{beginDate}'), to_date('{endDate}'), interval 1 day)) as calendarDate ")

  range_df.createOrReplaceTempView('dates_range')

  dim_df=spark.sql("""
  select
  year(calendarDate) * 10000 + month(calendarDate) * 100 + day(calendarDate) as date_int,
  calendarDate as calendar_date,
  year(calendarDate) AS calendar_year,
  date_format(calendarDate, 'MMMM') as calendar_month,
  month(calendarDate) as month_of_year,
  date_format(calendarDate, 'EEEE') as calendar_day,
  dayofweek(calendarDate) AS day_of_week,
  weekday(calendarDate) + 1 as day_of_week_start_monday,
  case
    when weekday(calendarDate) < 5 then 'Y'
    else 'N'
  end as is_week_day,
  dayofmonth(calendarDate) as day_of_month,
  case
    when calendarDate = last_day(calendarDate) then 'Y'
    else 'N'
  end as is_last_day_of_month,
  dayofyear(calendarDate) as day_of_year,
  weekofyear(calendarDate) as week_of_year_iso,
  quarter(calendarDate) as quarter_of_year
from
  dates_range
order by
  calendar_date
  """)


  dim_df.write \
   .format("delta") \
   .mode("append") \
   .option("mergeSchema", "true") \
   .option("path",dfm_reporting_path + "uwdfm_t_dates_dim_rpt")\
   .saveAsTable("reporting.uwdfm_t_dates_dim_rpt")

# COMMAND ----------

# MAGIC %md ###Pipeline Validation

# COMMAND ----------

dfm_activity.createOrReplaceTempView("dfmactivity")
dfm_param.createOrReplaceTempView("dfmparam")

dfm_input = spark.sql(""" 
select count(distinct a.parent_request_id,a.request_id,a.task_id) as dfm_record_count
from dfm.uwdfm_t_req_input_param a
join (
select parent_req_id, req_id, task_id
from reporting.dfm_uwdfm_t_activity_process_control_rpt_hist
union all
select
dapc.parent_req_id, dapc.request_id, dapc.task_id
from (
  select parent_req_id,
  --request_id,
  explode((split(request_id, '[_]'))) as request_id,
  task_id,
  status
  from
  dfm.uwdfm_t_activity_process_control
 ) dapc
where 1=1
and dapc.status="COMPLETE" 
and dapc.task_id like 'UW%'
) b on a.parent_request_id=b.parent_req_id  and a.request_id=b.req_id  and a.task_id=b.task_id
and coalesce(a.data_request_type,'DFM') ='DFM'
""")

reporting_input=spark.sql("""select count(distinct parent_request_id,request_id,task_id) as reporting_record_count from reporting.uwdfm_t_req_input_param_rpt""")

print("DFM distinct count: " + str(dfm_input.first()[0]))
print("Reporting distinct count: " + str(reporting_input.first()[0]))

if (dfm_input.first()[0]==reporting_input.first()[0]):
  print("The values matched between DFM Input param table and Reporting table in "+env_tag)
  #think about updating it in control tables as well  
  #dbutils.notebook.exit("Success")
else:  
  from datetime import datetime
  import pytz
  import json
  
  BOLD = "\033[1m"
  END = "\033[0m"

  ## Construct the Error Message

  tz=pytz.timezone('US/Central')
  currentDateAndTime = datetime.now(tz).strftime("%m/%d/%y %H:%M") 
  error_header=" Reporting Pipeline Out of Sync in "+env_tag+":"
  error_text=f"""
  Current Time: {currentDateAndTime} \n
  Error Msg: The records did not match between DFM Input table (dfm.uwdfm_t_req_input_param) and Reporting table (reporting.uwdfm_t_req_input_param_rpt) \n
  Number of distinct records in dfm.uwdfm_t_req_input_param : {str(dfm_input.first()[0])} \n
  Number of distinct records in reporting.uwdfm_t_req_input_param_rpt: {str(reporting_input.first()[0])} 
  """  
  error_msg = error_header +  "\n \n" + error_text
  print(error_msg)

  ## post the error_msg to Teams "DFM Reporting Pipeline" Channel

  import requests
  url = 'https://aetnao365.webhook.office.com/webhookb2/0442b632-bf06-413d-ba8d-0ea544fb1108@fabb61b8-3afe-4e75-b934-a47f782b8cd7/IncomingWebhook/e8f356a20f964763a3440f6548bbc154/884755d3-812e-4abb-98c1-4e0ce3fcabda'
  payload=json.dumps({"text":error_msg}, indent=2)
  
  headers = {'content-type': 'application/json'}
  r = requests.post(url, data=payload, headers=headers)
  print(r)
