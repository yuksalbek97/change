# Databricks notebook source
##########################-NOTES-########################################

# Date                 Created By               Reason
# 02/13/2024           Prashant Rai             US514122 (Hotfix to drop Pseudo Pharmacy Data)

# COMMAND ----------

# MAGIC %md ### CE Model Parameter - Incremental Preprocessing

# COMMAND ----------

###Getting Model Parameter Incremental Scenarios by comparing it with MShare Reporting Table
import json 

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


spark.sql(
"""
select distinct a.* from ce.model_parameters a 
left join reporting.ce_t_mshare_rpt b
on a.scenario_id=b.scenario_id
where b.scenario_id is null and a.status <> 'Failed'
"""
).createOrReplaceTempView("model_parameter_incremental")

# COMMAND ----------

###Getting the Index for each Years in JSON Payload
spark.sql(
"""
select  a.*,case when get_json_object(
              request_payload,
              '$.scenario.projected_years[0].year'
            )==1 then 0
            when get_json_object(
              request_payload,
              '$.scenario.projected_years[1].year'
            )==1 then 1
            when get_json_object(
              request_payload,
              '$.scenario.projected_years[2].year'
            )==1 then 2
            when get_json_object(
              request_payload,
              '$.scenario.projected_years[3].year'
            )==1 then 3
            end
            yr1_index,

        case when get_json_object(
              request_payload,
              '$.scenario.projected_years[0].year'
            )==2 then 0
            when get_json_object(
              request_payload,
              '$.scenario.projected_years[1].year'
            )==2 then 1
            when get_json_object(
              request_payload,
              '$.scenario.projected_years[2].year'
            )==2 then 2
            when get_json_object(
              request_payload,
              '$.scenario.projected_years[3].year'
            )==2 then 3
            end
            yr2_index, 

            case when get_json_object(
              request_payload,
              '$.scenario.projected_years[0].year'
            )==3 then 0
            when get_json_object(
              request_payload,
              '$.scenario.projected_years[1].year'
            )==3 then 1
            when get_json_object(
              request_payload,
              '$.scenario.projected_years[2].year'
            )==3 then 2
            when get_json_object(
              request_payload,
              '$.scenario.projected_years[3].year'
            )==3 then 3
            end
            yr3_index ,

            case when get_json_object(
              request_payload,
              '$.scenario.projected_years[0].year'
            )==0 then 0
            when get_json_object(
              request_payload,
              '$.scenario.projected_years[1].year'
            )==0 then 1
            when get_json_object(
              request_payload,
              '$.scenario.projected_years[2].year'
            )==0 then 2
            when get_json_object(
              request_payload,
              '$.scenario.projected_years[3].year'
            )==0 then 3
            end
            yr0_index             
            
            from model_parameter_incremental a
            """
            ).createOrReplaceTempView("model_parameter_flatten")


# COMMAND ----------

###Extract network_guid,formulary_guid,with_ees,open_specialty_arrangement,ondansetron,enoxaparin attributes for each year from payload.
### Identify the column to select/filter (out of 16 columns) from ce.revenue_summary table based on with_ees,open_specialty_arrangement,ondansetron,enoxaparin attribute values.
spark.sql(
    """
--   select
--     s.*,    
--     concat("rs.",
--       col_name,
--       " in ('SPECIALTY', 'SRX AT RETAIL')"
--     ) join_condition
--   FROM
--     (
      select
        tmp.*,    
        concat(
              'std_',
              case
                when yr1_with_ees = TRUE then 'w_ees'
                ELSE 'wo_ees'
              END,
              '_',
              case
                when yr1_open_specialty_arrangement = TRUE then 'open'
                ELSE 'excl'
              END,
              '_',
              case
                when yr1_ondansetron = TRUE
                and yr1_enoxaparin = TRUE THEN 'eno_ond'
                when yr1_ondansetron = FALSE
                and yr1_enoxaparin = FALSE THEN 'none'
                when yr1_ondansetron = FALSE
                and yr1_enoxaparin = TRUE THEN 'eno'
                else 'ond'
              END,
              '_id'
            )
        yr1_col_name,
        concat(
              'std_',
              case
                when yr2_with_ees = TRUE then 'w_ees'
                ELSE 'wo_ees'
              END,
              '_',
              case
                when yr2_open_specialty_arrangement = TRUE then 'open'
                ELSE 'excl'
              END,
              '_',
              case
                when yr2_ondansetron = TRUE
                and yr2_enoxaparin = TRUE THEN 'eno_ond'
                when yr2_ondansetron = FALSE
                and yr2_enoxaparin = FALSE THEN 'none'
                when yr2_ondansetron = FALSE
                and yr2_enoxaparin = TRUE THEN 'eno'
                else 'ond'
              END,
              '_id'
            )
        yr2_col_name,
        concat(
              'std_',
              case
                when yr3_with_ees = TRUE then 'w_ees'
                ELSE 'wo_ees'
              END,
              '_',
              case
                when yr3_open_specialty_arrangement = TRUE then 'open'
                ELSE 'excl'
              END,
              '_',
              case
                when yr3_ondansetron = TRUE
                and yr3_enoxaparin = TRUE THEN 'eno_ond'
                when yr3_ondansetron = FALSE
                and yr3_enoxaparin = FALSE THEN 'none'
                when yr3_ondansetron = FALSE
                and yr3_enoxaparin = TRUE THEN 'eno'
                else 'ond'
              END,
              '_id'
            )
        yr3_col_name,
        concat(
              'std_',
              case
                when yr0_with_ees = TRUE then 'w_ees'
                ELSE 'wo_ees'
              END,
              '_',
              case
                when yr0_open_specialty_arrangement = TRUE then 'open'
                ELSE 'excl'
              END,
              '_',
              case
                when yr0_ondansetron = TRUE
                and yr0_enoxaparin = TRUE THEN 'eno_ond'
                when yr0_ondansetron = FALSE
                and yr0_enoxaparin = FALSE THEN 'none'
                when yr0_ondansetron = FALSE
                and yr0_enoxaparin = TRUE THEN 'eno'
                else 'ond'
              END,
              '_id'
            )
        yr0_col_name
      FROM
        (
          select
            a.*,
            -- Year1
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr1_index,'].network_pricing_groups[0].network_guid')
            ) yr1_network_guid,
            case when 
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr1_index,'].network_pricing_groups[0].formulary_guid')              
            ) IS NOT NULL THEN
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr1_index,'].network_pricing_groups[0].formulary_guid')
            ) 
            ELSE
            -- when 
            -- get_json_object(
            --   request_payload,
            --   '$.scenario.projected_years[0].rebate_pricing_groups[0].formulary_guid'
            -- ) IS NOT NULL THEN
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr1_index,'].rebate_pricing_groups[0].formulary_guid')             
            ) 
            END
             yr1_formulary_guid,
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr1_index,'].specialty_pricing_groups[0].with_ees')              
            ) as yr1_with_ees,
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr1_index,'].specialty_pricing_groups[0].open_specialty_arrangement')              
            ) as yr1_open_specialty_arrangement,
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr1_index,'].specialty_pricing_groups[0].ondansetron')             
            ) as yr1_ondansetron,
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr1_index,'].specialty_pricing_groups[0].enoxaparin')               
            ) as yr1_enoxaparin,

            -- Year2
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr2_index,'].network_pricing_groups[0].network_guid')
            ) yr2_network_guid,
            case when 
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr2_index,'].network_pricing_groups[0].formulary_guid')              
            ) IS NOT NULL THEN
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr2_index,'].network_pricing_groups[0].formulary_guid')
            ) 
            ELSE
            -- when 
            -- get_json_object(
            --   request_payload,
            --   '$.scenario.projected_years[0].rebate_pricing_groups[0].formulary_guid'
            -- ) IS NOT NULL THEN
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr2_index,'].rebate_pricing_groups[0].formulary_guid')             
            ) 
            END
             yr2_formulary_guid,
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr2_index,'].specialty_pricing_groups[0].with_ees')              
            ) as yr2_with_ees,
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr2_index,'].specialty_pricing_groups[0].open_specialty_arrangement')              
            ) as yr2_open_specialty_arrangement,
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr2_index,'].specialty_pricing_groups[0].ondansetron')             
            ) as yr2_ondansetron,
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr2_index,'].specialty_pricing_groups[0].enoxaparin')               
            ) as yr2_enoxaparin,

             -- Year3
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr3_index,'].network_pricing_groups[0].network_guid')
            ) yr3_network_guid,
            case when 
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr3_index,'].network_pricing_groups[0].formulary_guid')              
            ) IS NOT NULL THEN
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr3_index,'].network_pricing_groups[0].formulary_guid')
            ) 
            ELSE
            -- when 
            -- get_json_object(
            --   request_payload,
            --   '$.scenario.projected_years[0].rebate_pricing_groups[0].formulary_guid'
            -- ) IS NOT NULL THEN
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr3_index,'].rebate_pricing_groups[0].formulary_guid')             
            ) 
            END
             yr3_formulary_guid,
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr3_index,'].specialty_pricing_groups[0].with_ees')              
            ) as yr3_with_ees,
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr3_index,'].specialty_pricing_groups[0].open_specialty_arrangement')              
            ) as yr3_open_specialty_arrangement,
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr3_index,'].specialty_pricing_groups[0].ondansetron')             
            ) as yr3_ondansetron,
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr3_index,'].specialty_pricing_groups[0].enoxaparin')               
            ) as yr3_enoxaparin,

            -- Year0
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr0_index,'].network_pricing_groups[0].network_guid')
            ) yr0_network_guid,
            case when 
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr0_index,'].network_pricing_groups[0].formulary_guid')              
            ) IS NOT NULL THEN
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr0_index,'].network_pricing_groups[0].formulary_guid')
            ) 
            ELSE
            -- when 
            -- get_json_object(
            --   request_payload,
            --   '$.scenario.projected_years[0].rebate_pricing_groups[0].formulary_guid'
            -- ) IS NOT NULL THEN
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr0_index,'].rebate_pricing_groups[0].formulary_guid')             
            ) 
            END
             yr0_formulary_guid,
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr0_index,'].specialty_pricing_groups[0].with_ees')              
            ) as yr0_with_ees,
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr0_index,'].specialty_pricing_groups[0].open_specialty_arrangement')              
            ) as yr0_open_specialty_arrangement,
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr0_index,'].specialty_pricing_groups[0].ondansetron')             
            ) as yr0_ondansetron,
            get_json_object(
              request_payload,
              concat('$.scenario.projected_years[',yr0_index,'].specialty_pricing_groups[0].enoxaparin')               
            ) as yr0_enoxaparin
			
          from            
            model_parameter_flatten a
        ) tmp
  --  ) s
    """
).createOrReplaceTempView("model_parameter_tmp")

# COMMAND ----------

### this step is to pick only the latest req_id if there are more than one req_id for a scenario.
spark.sql(
"""
select * from 
(
select a.*,
b.inp_load_ts,
rank() over(PARTITION BY scenario_id order by b.inp_load_ts desc,a.created_at desc) rank_req_id
from model_parameter_tmp a
inner join dfm.uwdfm_t_req_input_param b
on a.uw_req_id=b.parent_request_id
where a.status <> 'Failed'
)
where rank_req_id=1
"""
).createOrReplaceTempView("model_parameter")


# COMMAND ----------

### Transposing the 6 attribute values from columnwise to rowwise.
spark.sql(
"""
select distinct opportunity_id,
uw_req_id,
scenario_id,
created_at,
load_ts,
stack(4,
'1',yr1_network_guid,yr1_formulary_guid,yr1_with_ees,yr1_open_specialty_arrangement,yr1_ondansetron,yr1_enoxaparin,yr1_col_name,
'2',yr2_network_guid,yr2_formulary_guid,yr2_with_ees,yr2_open_specialty_arrangement,yr2_ondansetron,yr2_enoxaparin,yr2_col_name,
'3',yr3_network_guid,yr3_formulary_guid,yr3_with_ees,yr3_open_specialty_arrangement,yr3_ondansetron,yr3_enoxaparin,yr3_col_name,
'0',yr0_network_guid,yr0_formulary_guid,yr0_with_ees,yr0_open_specialty_arrangement,yr0_ondansetron,yr0_enoxaparin,yr0_col_name
) 
as (proj_year,network_guid,formulary_guid,with_ees,open_specialty_arrangement,ondansetron,enoxaparin,col_name)
from model_parameter a
"""
).createOrReplaceTempView("model_parameter_unpivot")

# COMMAND ----------

# MAGIC %md ### CE Revenue Summary - Incremental Preprocessing

# COMMAND ----------

## Getting Revenue Summary data just for the Incremental Scenarios
spark.sql(
"""  
select a.* from ce.revenue_summary a 
inner join model_parameter_incremental b
on a.scenario_id=b.scenario_id
"""
).createOrReplaceTempView("revenue_summary_incremental")

# COMMAND ----------

### Getting the speciality claims count from ce.revenue_summary table
spark.sql(
"""    
select
uw_req_id as uw_req_id,
scenario_id as scenario_id,
opportunity_id as opportunity,
pharmacy_group as pharmacy_chain,
pharmacy_bucket as Pharmacy,
srx_pricing_group_id,
proj_year,
network_guid,
formulary_guid,
business_type_id,
sum(case when std_wo_ees_open_none_id in ('SPECIALTY', 'SRX AT RETAIL') then num_claims else 0 end) as std_wo_ees_open_none_id,
sum(case when std_w_ees_open_none_id in ('SPECIALTY', 'SRX AT RETAIL') then num_claims else 0 end) as std_w_ees_open_none_id,
sum(case when std_wo_ees_excl_none_id in ('SPECIALTY', 'SRX AT RETAIL') then num_claims else 0 end) as std_wo_ees_excl_none_id,
sum(case when std_w_ees_excl_none_id in ('SPECIALTY', 'SRX AT RETAIL') then num_claims else 0 end) as std_w_ees_excl_none_id,

sum(case when std_wo_ees_open_ond_id in ('SPECIALTY', 'SRX AT RETAIL') then num_claims else 0 end) as std_wo_ees_open_ond_id,
sum(case when std_w_ees_open_ond_id in ('SPECIALTY', 'SRX AT RETAIL') then num_claims else 0 end) as std_w_ees_open_ond_id,
sum(case when std_wo_ees_excl_ond_id in ('SPECIALTY', 'SRX AT RETAIL') then num_claims else 0 end) as std_wo_ees_excl_ond_id,
sum(case when std_w_ees_excl_ond_id in ('SPECIALTY', 'SRX AT RETAIL') then num_claims else 0 end) as std_w_ees_excl_ond_id,

sum(case when std_wo_ees_open_eno_id in ('SPECIALTY', 'SRX AT RETAIL') then num_claims else 0 end) as std_wo_ees_open_eno_id,
sum(case when std_w_ees_open_eno_id in ('SPECIALTY', 'SRX AT RETAIL') then num_claims else 0 end) as std_w_ees_open_eno_id,
sum(case when std_wo_ees_excl_eno_id in ('SPECIALTY', 'SRX AT RETAIL') then num_claims else 0 end) as std_wo_ees_excl_eno_id,
sum(case when std_w_ees_excl_eno_id in ('SPECIALTY', 'SRX AT RETAIL') then num_claims else 0 end) as std_w_ees_excl_eno_id,

sum(case when std_wo_ees_open_eno_ond_id in ('SPECIALTY', 'SRX AT RETAIL') then num_claims else 0 end) as std_wo_ees_open_eno_ond_id,
sum(case when std_w_ees_open_eno_ond_id in ('SPECIALTY', 'SRX AT RETAIL') then num_claims else 0 end) as std_w_ees_open_eno_ond_id,
sum(case when std_wo_ees_excl_eno_ond_id in ('SPECIALTY', 'SRX AT RETAIL') then num_claims else 0 end) as std_wo_ees_excl_eno_ond_id,
sum(case when std_w_ees_excl_eno_ond_id in ('SPECIALTY', 'SRX AT RETAIL') then num_claims else 0 end) as std_w_ees_excl_eno_ond_id

from revenue_summary_incremental
group by 1,2,3,4,5,6,7,8,9,10
"""
).createOrReplaceTempView("revenue_summary")

# COMMAND ----------

## Transposing it from column-wise to row-wise to make it easier to join with model_parameter table.
spark.sql(
"""
select
uw_req_id,
scenario_id,
opportunity,
pharmacy_chain,
Pharmacy,
network_guid,
formulary_guid,
srx_pricing_group_id,
proj_year,
business_type_id,
stack (16,
"std_wo_ees_open_none_id", std_wo_ees_open_none_id,
"std_w_ees_open_none_id", std_w_ees_open_none_id,
"std_wo_ees_excl_none_id", std_wo_ees_excl_none_id,
"std_w_ees_excl_none_id", std_w_ees_excl_none_id,
"std_wo_ees_open_ond_id", std_wo_ees_open_ond_id,
"std_w_ees_open_ond_id", std_w_ees_open_ond_id,
"std_wo_ees_excl_ond_id", std_wo_ees_excl_ond_id,
"std_w_ees_excl_ond_id", std_w_ees_excl_ond_id,
"std_wo_ees_open_eno_id", std_wo_ees_open_eno_id,
"std_w_ees_open_eno_id", std_w_ees_open_eno_id,
"std_wo_ees_excl_eno_id", std_wo_ees_excl_eno_id,
"std_w_ees_excl_eno_id", std_w_ees_excl_eno_id,
"std_wo_ees_open_eno_ond_id", std_wo_ees_open_eno_ond_id,
"std_w_ees_open_eno_ond_id", std_w_ees_open_eno_ond_id,
"std_wo_ees_excl_eno_ond_id", std_wo_ees_excl_eno_ond_id,
"std_w_ees_excl_eno_ond_id", std_w_ees_excl_eno_ond_id
)
from revenue_summary
"""
).createOrReplaceTempView("revenue_summary_unpivot")

# COMMAND ----------

# MAGIC %md ### MShare View Creation

# COMMAND ----------

# MShare view creation by joining the above sources - preprocessed model_parameter, revenue_summary tables.
spark.sql(
"""
select
rs.uw_req_id,
rs.scenario_id,
rs.opportunity,
rs.pharmacy_chain,
rs.Pharmacy,
sum(col1) as Rxs,
rs.srx_pricing_group_id,
rs.business_type_id,
mp.with_ees,
mp.open_specialty_arrangement,
mp.ondansetron,
mp.enoxaparin,
mp.proj_year,
DATE(mp.created_at) as ce_created_at,
mp.load_ts as load_timestamp,
from_utc_timestamp(current_timestamp(), 'America/Chicago') updated_at
from
  revenue_summary_unpivot rs inner join
  model_parameter_unpivot mp 
  on mp.scenario_id = rs.scenario_id 
    and mp.uw_req_id = rs.uw_req_id
    and mp.network_guid = rs.network_guid
    and mp.formulary_guid = rs.formulary_guid
   -- and mp.status <> 'Failed'
    and mp.col_name=rs.col0
    and mp.proj_year=rs.proj_year
  
group by
1,2,3,4,5,7,8,9,10,11,12,13,14,15
"""
).createOrReplaceTempView("srx_mshare")

# COMMAND ----------

spark.sql(
"""
select distinct
rs.uw_req_id,
rs.scenario_id,
rs.opportunity,
--rs.pharmacy_chain,
--rs.Pharmacy,
rp.pharmacy_bucket as Pharmacy,
--sum(col1) as Rxs,
total_claims as Rxs,
rs.srx_pricing_group_id,
rs.with_ees,
rs.open_specialty_arrangement,
rs.ondansetron,
rs.enoxaparin,
rs.proj_year,
rs.ce_created_at,
rs.load_timestamp,
rs.updated_at
from srx_mshare rs
join
(
select 
ppb.scenario_id,ppb.uw_req_id,ppb.proj_year,ppb.pharmacy_bucket,ppb.opportunity_id,ppb.business_type_id,
--ppb.channel_group
--sum(ppb.incl_brand_revenue + ppb.excl_brand_revenue + ppb.incl_generic_revenue + ppb.excl_generic_revenue) total_revenue
sum(excl_brand_claims+incl_brand_claims+excl_generic_claims+incl_generic_claims+incl_nonmac_claims) total_claims
from ce.pnl_pharmacy_bucket ppb
where 1=1 
and ppb.channel_group = 'SRx at retail'
group by 1,2,3,4,5,6

union

select 
psd.scenario_id,psd.uw_req_id,psd.proj_year,'CVS CAREMARK'pharmacy_bucket,psd.opportunity_id,psd.business_type_id,
--'CVS' channel_group
--sum(psd.specialty_revenue) total_revenue,
sum(total_claims) total_claims
from ce.pnl_specialty_drug psd
where 1=1  
group by 1,2,3,4,5,6  
) rp on rs.opportunity=rp.opportunity_id and rs.uw_req_id=rp.uw_req_id and rs.scenario_id=rp.scenario_id and rs.proj_year=rp.proj_year and rs.business_type_id=rp.business_type_id
"""
).createOrReplaceTempView("srx_mshare_combined")

# COMMAND ----------

# MAGIC %md ###Reporting MShare Table - Incremental Refresh

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS spark_catalog.reporting.ce_t_mshare_rpt (
# MAGIC   uw_req_id STRING,
# MAGIC   scenario_id STRING,
# MAGIC   opportunity STRING,
# MAGIC   Pharmacy STRING,
# MAGIC   Rxs DOUBLE,
# MAGIC   srx_pricing_group_id STRING,
# MAGIC   with_ees STRING,
# MAGIC   open_specialty_arrangement STRING,
# MAGIC   ondansetron STRING,
# MAGIC   enoxaparin STRING,
# MAGIC   proj_year INT,
# MAGIC   ce_created_at STRING,
# MAGIC   load_timestamp TIMESTAMP,
# MAGIC   updated_at TIMESTAMP)
# MAGIC USING delta
# MAGIC PARTITIONED BY (ce_created_at, opportunity, uw_req_id, scenario_id, proj_year)
# MAGIC LOCATION 'dbfs:/mnt/bronze/afp/dfm/reporting/ce_t_mshare_rpt'

# COMMAND ----------

# DBTITLE 1,Insert into MShare table the records from View
# MAGIC %sql
# MAGIC -- Incremental load into MShare reporting table.
# MAGIC insert into reporting.ce_t_mshare_rpt
# MAGIC select * from srx_mshare_combined;
