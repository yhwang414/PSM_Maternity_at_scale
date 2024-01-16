# Author: Yeon Mi Hwang  
# Date: 01/15/2024
# Disclaimer : This code was originally written in databricks workspace. 
# Objective of ATC_generate.py : To link ATC categories to medication records and generate tree files 
# Workflow of ATC_generate.py 
## 1. load necessary packages and functions 
## 2. generate tree data 
## 3. generate count file 




# ----------------------------------------------------------------------------
# 1. load necessary packages and functions 

import CEDA_Tools_v1_1.load_ceda_etl_tools
# Databricks notebook source
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import *

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)


# ----------------------------------------------------------------------------
# 2. generate tree data 
# 2.1 load ATC mapping datatable (PSJH medication id to ATC category)

mapping_df = spark.sql("SELECT * FROM rdp_phi_sandbox.hadlock_medication_id_atc_mapping")


# 2.2 generate tree df 
first = []
second = []
third = []
fourth = []
fifth = []
for i in mapping_df.columns:
  if len(i) == 1:
    first.append(i)
  elif len(i) == 3:
    second.append(i)
  elif len(i) == 4:
    third.append(i)
  elif len(i) == 5:
    fourth.append(i)
  elif len(i) == 7:
    fifth.append(i)

ATC_list = first + second + third + fourth + fifth #list of ATC category 


# generate tree dataframe 
import pandas as pd
dicts = {}
for a in fifth:
  dicts[a] = a[0:5]
for b in fourth:
  dicts[b] = b[0:4]
for c in third:
  dicts[c] = c[0:3]
for d in second:
  dicts[d] = d[0:1]
for e in first:
  dicts[e] = 'Root'

ATC_df = pd.DataFrame(list(dicts.items()))

# ----------------------------------------------------------------------------
# 3. generate count file 
# 3.1. get all medication record for moms during their pregnancy 
# 3.2. Join mapping table 
# 3.3. aggregate 


# 3.1. get all medication record for moms during their pregnancy  
spark.sql("REFRESH TABLE rdp_phi_sandbox.yh_maternity_2022")
mom = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_2022")
mom = mom.withColumn("admission_datetime_filled", F.when(F.col("admission_datetime_max").isNull(), F.col('ob_delivery_delivery_date'))
                                       .otherwise(F.col("admission_datetime_max")))
mom_meds2 = get_medication_orders(cohort_df=mom,
                      include_cohort_columns=['patient_id', 'gestational_days', 'lmp', 'ob_delivery_delivery_date', 'SGA', 'LBW', 'vLBW', 'preterm_category','admission_datetime_filled'],
                      filter_string='end_date > lmp AND start_date < date_sub(ob_delivery_delivery_date,2)',
                      omit_columns=medication_orders_default_columns_to_omit(),
                      add_cc_columns=[]).filter(F.col('order_class')!='Historical Med').drop(*['order_description', 'order_class', 'order_set', 'order_status', 'order_priority', 'requested_instant_utc', 'authorizing_prov_id', 'department_id', 'controlled', 'recorded_datetime', 'due_datetime', 'scheduled_on_datetime', 'scheduled_for_datetime']).distinct()


mom_meds2.createOrReplaceTempView("med2")



med2 = spark.sql("""
SELECT *
FROM med2
WHERE short_name NOT LIKE '%lactated%' AND 
short_name NOT LIKE '%ringers%' AND 
short_name NOT LIKE '%sodium chloride%' AND 
short_name NOT LIKE '%dextrose%' AND 
short_name NOT LIKE '%electrolyte%' AND
short_name NOT LIKE '%lactated ringers%' AND
name NOT LIKE '%IV%' AND 
name NOT LIKE '%RV%' AND
name NOT LIKE '%Aloe%'
""")



med2_ip = med2.filter(F.col('order_mode')=='Inpatient')
med2_op = med2.filter(F.col('order_mode')=='Outpatient')



def get_exposure_period(exposure_date):
  if exposure_date < -180:
    return -1
  elif -180 <= exposure_date < 0:
    return 0
  elif 0 <= exposure_date < 84:
    return 1 
  elif 84 <= exposure_date < 189:
    return 2
  elif 189 <= exposure_date <= 320:
    return 3
  elif 320 < exposure_date:
    return 4
  else:
    return None 
get_exposure_period_udf = F.udf(lambda exp: get_exposure_period(exp), StringType())

def get_exposure0_duration(orderstart_lmp, orderend_lmp):
  if (orderstart_lmp < -180) and (orderend_lmp < -180):
    return 0
  elif (orderstart_lmp < -180) and (-180 <= orderend_lmp < 0):
    return orderend_lmp - 180 + 1
  elif (orderstart_lmp < -180) and (0 <= orderend_lmp):
    return 180
  elif (-180 <= orderstart_lmp < 0) and  (-180 <= orderend_lmp < 0):
    return orderend_lmp - orderstart_lmp + 1
  elif (-180 <= orderstart_lmp < 0) and (0 <= orderend_lmp):
    return 0 - orderstart_lmp + 1
  elif (0 <= orderstart_lmp) and (0 <= orderend_lmp):
    return 0
  else:
    return None
def get_exposure1_duration(orderstart_lmp, orderend_lmp):
  if ( orderstart_lmp < 0) and ( orderend_lmp < 0):
    return 0
  elif ( orderstart_lmp < 0) and (0 <= orderend_lmp < 84):
    return orderend_lmp + 1 
  elif ( orderstart_lmp < 0) and (84 <= orderend_lmp ):
    return 84 
  elif (0 <= orderstart_lmp < 84) and (0 <= orderend_lmp < 84):
    return orderend_lmp - orderstart_lmp + 1
  elif (0 <= orderstart_lmp < 84) and (84 <= orderend_lmp):
    return 84 - orderstart_lmp + 1
  elif (84 <= orderstart_lmp) and (84 <= orderend_lmp):
    return 0
  else:
    return 0
  
def get_exposure2_duration(orderstart_lmp, orderend_lmp):
  if (orderstart_lmp < 84) and (orderend_lmp < 84):
    return 0
  elif (orderstart_lmp < 84) and (84 <= orderend_lmp < 189):
    return orderend_lmp - 84 + 1 
  elif (91 <= orderstart_lmp < 189) and (84 <= orderend_lmp < 189):
    return orderend_lmp - orderstart_lmp + 1
  elif ( orderstart_lmp < 84) and (189 <= orderend_lmp ):
    return 189-84
  elif (84 <= orderstart_lmp < 189) and (189 <= orderend_lmp):
    return 189 - orderstart_lmp + 1
  elif (189 <= orderstart_lmp) and (189 <= orderend_lmp):
    return 0
  else:
    return 0

def get_exposure3_duration(orderstart_lmp, orderend_lmp, gestational_days):
  if (orderstart_lmp < 189) and (orderend_lmp < 189):
    return 0
  elif (orderstart_lmp < 189) and (189 <= orderend_lmp <= gestational_days):
    return orderend_lmp -189 + 1 
  elif (orderstart_lmp < 189) and (189 <= gestational_days <= orderend_lmp) :
    return gestational_days -189 + 1 
  elif (189 <= orderstart_lmp <= gestational_days) and (189 <= orderend_lmp <= gestational_days):
    return orderend_lmp - orderstart_lmp + 1
  elif (189 <= orderstart_lmp <= gestational_days) and (gestational_days <= orderend_lmp):
    return gestational_days - orderstart_lmp + 1
  else:
    return 0

get_exposure0_duration_udf = F.udf(lambda start, end: get_exposure0_duration(start, end), StringType())
get_exposure1_duration_udf = F.udf(lambda start, end: get_exposure1_duration(start, end), StringType())
get_exposure2_duration_udf = F.udf(lambda start, end: get_exposure2_duration(start, end), StringType())
get_exposure3_duration_udf = F.udf(lambda start, end, ga: get_exposure3_duration(start, end, ga), StringType())

def map_to_preterm_5labels(gestational_age):
    if(gestational_age is None):
      return None
    elif(gestational_age >= 273):
      return 'term'
    elif(gestational_age >= 259):
      return 'early term'
    elif(gestational_age >= 224):
      return 'moderate_late preterm'
    elif(gestational_age >= 196):
      return 'very_preterm'
    elif(gestational_age >= 140):
      return 'extremely_preterm'
    else:
      return 'spontaneous abortion'
map_to_preterm_5labels_udf = F.udf(lambda ga: map_to_preterm_5labels(ga), StringType())
def map_to_preterm_2labels(gestational_age):
    if(gestational_age is None):
      return None
    elif(gestational_age >= 259):
      return 'term'
    elif(gestational_age < 259) and (gestational_age >= 140):
      return 'preterm'
    else:
      return 'spontaneous abortion'
map_to_preterm_2labels_udf = F.udf(lambda ga: map_to_preterm_2labels(ga), StringType())




import re
def replace_strings(str_value):
  first = str_value.replace("max(", "")
  first = first.replace("sum(", "")
  end = first.replace(")", "")
  return end
 
def generate_col_dicts(old_name_list, new_name_list):
  return_dict = {}
  for n in range(0,len(old_name_list)):
    return_dict[old_name_list[n]] = new_name_list[n]
  return return_dict
    
import pyspark.sql.functions as F
 
def rename_columns(df, columns):
    if isinstance(columns, dict):
        return df.select(*[F.col(col_name).alias(columns.get(col_name, col_name)) for col_name in df.columns])
    else:
        raise ValueError("'columns' should be a dict, like {'old_name_1':'new_name_1', 'old_name_2':'new_name_2'}")



def get_medinfo_cohort(cohort_df):
  partition_columns = ['instance', 'pat_id', 'lmp', 'gestational_days', 'ob_delivery_delivery_date', 'medication_id', 'medication_description', 'term_type', 'rxnorm_code']
  agg_cols = {'exposure0_duration' : 'sum',
  'exposure1_duration' : 'sum',
  'exposure2_duration' : 'sum',
  'exposure3_duration' : 'sum',
  'exposure_total_duration' : 'sum',
  'exposure0_status' : 'max',
  'exposure1_status' : 'max',
  'exposure2_status' : 'max',
  'exposure3_status' : 'max',
  'exposure_total_status':'max'}
  temp_df = cohort_df.withColumn('orderstart_lmp', F.datediff(F.col('start_date'), F.col('lmp'))) .\
                        withColumn('orderend_lmp',  F.datediff(F.col('end_date'), F.col('lmp'))) .\
                        withColumn('order_duration', F.datediff(F.col('end_date'), F.col('start_date'))+1) .\
                        withColumn('orderstart_lmp_period', get_exposure_period_udf(F.col('orderstart_lmp'))) .\
                        withColumn('orderstart_lmp_period', get_exposure_period_udf(F.col('orderstart_lmp'))) .\
                        withColumn('exposure0_duration', get_exposure0_duration_udf(F.col('orderstart_lmp'), F.col('orderend_lmp'))) .\
                        withColumn('exposure1_duration', get_exposure1_duration_udf(F.col('orderstart_lmp'), F.col('orderend_lmp'))) .\
                        withColumn('exposure2_duration', get_exposure2_duration_udf(F.col('orderstart_lmp'), F.col('orderend_lmp'))) .\
                        withColumn('exposure3_duration', get_exposure3_duration_udf(F.col('orderstart_lmp'), F.col('orderend_lmp'), F.col('gestational_days'))) .\
                        withColumn('exposure_total_duration', (F.col('exposure1_duration')+F.col('exposure2_duration')+F.col('exposure3_duration'))) .\
                        withColumn('exposure0_status', F.when(F.col('exposure0_duration') > 0,1).otherwise(0)) .\
                        withColumn('exposure1_status', F.when(F.col('exposure1_duration') > 0,1).otherwise(0)) .\
                        withColumn('exposure2_status', F.when(F.col('exposure2_duration') > 0,1).otherwise(0)) .\
                        withColumn('exposure3_status', F.when(F.col('exposure3_duration') > 0,1).otherwise(0)) .\
                        withColumn('exposure_total_status', F.when(F.col('exposure_total_duration') > 0,1).otherwise(0))
  temp_df2 = temp_df.groupby(partition_columns).agg(agg_cols)
  old_name_list = list(set(temp_df2.columns)-set(partition_columns))
  new_name_list = [replace_strings(i) for i in old_name_list]
  rename_cols = generate_col_dicts(old_name_list, new_name_list)
  result_df = rename_columns(temp_df2, rename_cols)
  return result_df


med2_ip = get_medinfo_cohort(med2_ip)
med2_op = get_medinfo_cohort(med2_op)
med2 = get_medinfo_cohort(med2)



med2_mapped = med2.join(mapping_df, ['medication_id', 'instance', 'medication_description', 'term_type', 'rxnorm_code'])
med2_ip_mapped = med2_ip.join(mapping_df, ['medication_id', 'instance', 'medication_description', 'term_type', 'rxnorm_code'])
med2_op_mapped = med2_op.join(mapping_df, ['medication_id', 'instance', 'medication_description', 'term_type', 'rxnorm_code'])


spark.sql("DROP TABLE IF EXISTS rdp_phi_sandbox.yh_maternity_meds_ATC_mapped")
write_data_frame_to_sandbox(med2_mapped, "yh_maternity_meds_ATC_mapped", sandbox_db='rdp_phi_sandbox', replace=True)



spark.sql("DROP TABLE IF EXISTS rdp_phi_sandbox.yh_maternity_meds_ip_ATC_mapped")
write_data_frame_to_sandbox(med2_ip_mapped, "yh_maternity_meds_ip_ATC_mapped", sandbox_db='rdp_phi_sandbox', replace=True)



spark.sql("DROP TABLE IF EXISTS rdp_phi_sandbox.yh_maternity_meds_op_ATC_mapped")
write_data_frame_to_sandbox(med2_op_mapped, "yh_maternity_meds_op_ATC_mapped", sandbox_db='rdp_phi_sandbox', replace=True)



meds_total = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_meds_ATC_mapped").filter(F.col('exposure_total_status')==1)
meds_ip_total = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_meds_ip_ATC_mapped").filter(F.col('exposure_total_status')==1)
meds_op_total = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_meds_op_ATC_mapped").filter(F.col('exposure_total_status')==1)


# 3.3. aggregate 

partition_columns = ['pat_id', 'instance', 'lmp', 'ob_delivery_delivery_date', 'admission_datetime_filled']


def intersection(lst1, lst2):
    lst3 = [value for value in lst1 if value in lst2]
    return lst3



joining_columns = intersection(mom.columns, meds_total.columns)
joining_columns



cohort_df_joined_meds = mom.join(meds_total, joining_columns, 'left')
cohort_df_joined_meds_ip = mom.join(meds_ip_total, joining_columns, 'left')
cohort_df_joined_meds_op = mom.join(meds_op_total, joining_columns, 'left')

name_list = first + second + third + fourth + fifth
agg_cols =  {}
for i in name_list: 
  agg_cols[i] = 'max'
meds_ag = cohort_df_joined_meds.groupBy(*joining_columns).agg(agg_cols)
meds_ag_ip = cohort_df_joined_meds_ip.groupBy(*joining_columns).agg(agg_cols)
meds_ag_op = cohort_df_joined_meds_op.groupBy(*joining_columns).agg(agg_cols)




max_name_list = [i for i in meds_ag.columns if i not in joining_columns]
name_dict = {}
for n in range(0,len(max_name_list)):
    name_dict[max_name_list[n]] = replace_strings(max_name_list[n])



meds_ag = rename_columns(meds_ag, name_dict)
meds_ag_ip = rename_columns(meds_ag_ip, name_dict)
meds_ag_op = rename_columns(meds_ag_op, name_dict)



reorder_columns = joining_columns + name_list
meds_status_df = meds_ag.fillna(False, subset=list(name_dict.values())).select(*reorder_columns)
meds_ip_status_df = meds_ag_ip.fillna(False, subset=list(name_dict.values())).select(*reorder_columns)
meds_op_status_df = meds_ag_op.fillna(False, subset=list(name_dict.values())).select(*reorder_columns)


table_name = "yh_maternity_ATC_med_status"
spark.sql("DROP TABLE IF EXISTS rdp_phi_sandbox.{0}".format(table_name))
write_data_frame_to_sandbox(meds_status_df, table_name , 'rdp_phi_sandbox', replace = True)



table_name = "yh_maternity_ATC_med_ip_status"
spark.sql("DROP TABLE IF EXISTS rdp_phi_sandbox.{0}".format(table_name))
write_data_frame_to_sandbox(meds_ip_status_df, table_name , 'rdp_phi_sandbox', replace = True)



table_name = "yh_maternity_ATC_med_op_status"
spark.sql("DROP TABLE IF EXISTS rdp_phi_sandbox.{0}".format(table_name))
write_data_frame_to_sandbox(meds_op_status_df, table_name , 'rdp_phi_sandbox', replace = True)



check = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_ATC_med_status")
check_pd = check.toPandas()





def intersection(lst1, lst2):
    lst3 = [value for value in lst1 if value in lst2]
    return lst3


atc_dicts = {}
for fc in first:
  atc_dicts[fc] = [atc for atc in mapping_df.columns if atc.startswith(fc)]


def melt(
        df: DataFrame, 
        id_vars: Iterable[str], value_vars: Iterable[str], 
        var_name: str="variable", value_name: str="value") -> DataFrame:
    """Convert :class:`DataFrame` from wide to long format."""

    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = array(*(
        struct(lit(c).alias(var_name), col(c).alias(value_name)) 
        for c in value_vars))

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", explode(_vars_and_vals))

    cols = id_vars + [
            col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)



partition_columns = ['pat_id', 'instance', 'lmp', 'gestational_days']
melted_df = []
op_melted_df = []
ip_melted_df = []
for fc in first:
  meds_df = melt(meds, id_vars = partition_columns, value_vars = atc_dicts[fc])
  aggregated_med_df = aggregate_data(meds_df, partition_columns = partition_columns + ['variable'], aggregation_columns = {'value':'max'})
  melted_df.append(aggregated_med_df)
  meds_ip_df = melt(meds_ip, id_vars = partition_columns, value_vars = atc_dicts[fc])
  aggregated_ip_df = aggregate_data(meds_ip_df, partition_columns = partition_columns + ['variable'], aggregation_columns = {'value':'max'})
  ip_melted_df.append(aggregated_ip_df)
  meds_op_df = melt(meds_op, id_vars = partition_columns, value_vars = atc_dicts[fc])
  aggregated_op_df = aggregate_data(meds_op_df, partition_columns = partition_columns + ['variable'], aggregation_columns = {'value':'max'})
  op_melted_df.append(aggregated_op_df)




for md, fc in zip(op_melted_df, first):
  write_data_frame_to_sandbox(md, "yh_maternity_meds_op_mapping_melted_aggregated_{0}".format(fc), sandbox_db='rdp_phi_sandbox', replace=True)

# COMMAND ----------

for md, fc in zip(ip_melted_df, first):
  write_data_frame_to_sandbox(md, "yh_maternity_meds_ip_mapping_melted_aggregated_{0}".format(fc), sandbox_db='rdp_phi_sandbox', replace=True)

# COMMAND ----------

for md, fc in zip(melted_df, first):
  spark.sql("DROP TABLE IF EXISTS rdp_phi_sandbox.yh_maternity_meds_mapping_melted_aggregated_{0}".format(fc))
  write_data_frame_to_sandbox(md, "yh_maternity_meds_mapping_melted_aggregated_{0}".format(fc), sandbox_db='rdp_phi_sandbox', replace=True)

# COMMAND ----------

saved_op_melted_df = []
saved_ip_melted_df = []
saved_melted_df = []
partition_columns = ['pat_id', 'instance', 'lmp', 'gestational_days']
for fc in first:
  op_df = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_meds_op_mapping_melted_aggregated_{0}".format(fc))
  op_df = op_df.withColumn('value_max',F.col('value_max').cast(DoubleType()))
  op_df_pivoted = op_df.groupBy(*partition_columns).pivot('variable').max("value_max")
  joined_op = mom.join()
  saved_op_melted_df.append(op_df_pivoted)
  ip_df = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_meds_ip_mapping_melted_aggregated_{0}".format(fc))
  ip_df = ip_df.withColumn('value_max',F.col('value_max').cast(DoubleType()))
  ip_df_pivoted = ip_df.groupBy(*partition_columns).pivot('variable').max("value_max")
  saved_ip_melted_df.append(ip_df_pivoted)
  df = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_meds_mapping_melted_aggregated_{0}".format(fc))
  df = df.withColumn('value_max',F.col('value_max').cast(DoubleType()))
  df_pivoted = df.groupBy(*partition_columns).pivot('variable').max("value_max")
  saved_melted_df.append(df_pivoted)


# DBTITLE 1,Join with medication mapping datatable 
mom_meds_op_mapping = mom_meds_op.join(mapping_df.select(['instance','medication_id']+fifth),['instance', 'medication_id'], 'left')
mom_meds_ip_mapping = mom_meds_ip.join(mapping_df.select(['instance','medication_id']+fifth),['instance', 'medication_id'], 'left')

# COMMAND ----------

write_data_frame_to_sandbox(mom_meds_op_mapping , "yh_maternity_meds_op_mapping", sandbox_db='rdp_phi_sandbox', replace=True)

# COMMAND ----------

write_data_frame_to_sandbox(mom_meds_ip_mapping , "yh_maternity_meds_ip_mapping", sandbox_db='rdp_phi_sandbox', replace=True)

# COMMAND ----------

spark.sql("REFRESH TABLE rdp_phi_sandbox.yh_maternity_meds_op_mapping")
op_mapping =   spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_meds_op_mapping")

# COMMAND ----------

spark.sql("REFRESH TABLE rdp_phi_sandbox.yh_maternity_meds_ip_mapping")
ip_mapping =   spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_meds_ip_mapping")


