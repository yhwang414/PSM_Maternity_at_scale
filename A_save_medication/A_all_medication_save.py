# Author: Yeon Mi Hwang  
# Date: 01/15/2024
# Disclaimer : This code was originally written in databricks workspace. 
# Objective of A_all_medication_save.py : To ETL all relevant medication records 
# Workflow of A_all_medication_save.py 
## 1. load necessary packages and functions 
## 2. load cohort 
## 3. load relevant medication records 
## 4. Clean up medication records 
## 5. Save medication records 

# ----------------------------------------------------------------------------
# 1. load necessary packages and functions 

import CEDA_Tools_v1_1.load_ceda_etl_tools
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import array, col, explode, lit
from pyspark.sql.functions import create_map
from pyspark.sql import DataFrame
from typing import Iterable 
from itertools import chain

def melt(
        df: DataFrame, 
        id_vars: Iterable[str], value_vars: Iterable[str], 
        var_name: str="variable", value_name: str="value") -> DataFrame:
    """Convert :class:`DataFrame` from wide to long format."""

    # Create map<key: value>
    _vars_and_vals = create_map(
        list(chain.from_iterable([
            [lit(c), col(c)] for c in value_vars]
        ))
    )

    _tmp = df.select(*id_vars, explode(_vars_and_vals)) \
        .withColumnRenamed('key', var_name) \
        .withColumnRenamed('value', value_name)

    return _tmp

# ----------------------------------------------------------------------------

# 2. load cohort (maternity cohort 20130101-20221231)

mom = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_2022")



#get a column with admission date for labor and delivery 
from pyspark.sql.functions import expr
mom = mom.withColumn("admission_datetime_filled", F.when(F.col("admission_datetime_max").isNull(), F.col('ob_delivery_delivery_date'))
                                       .otherwise(F.col("admission_datetime_max")))



# ----------------------------------------------------------------------------

# 3. load medication record  
# difference between med1 and med2 (this was done to avoid the medication used for labor and delivery)
# med1 : used admission_datetime_filled as the cutoff date (has some high missingness)
# med2 : used delivery date - 2 as the cutoff date 
# we ended up using med2 


mom_meds = get_medication_orders(cohort_df=mom,
                      include_cohort_columns=['patient_id', 'gestational_days', 'lmp', 'ob_delivery_delivery_date', 'SGA', 'LBW', 'vLBW', 'preterm_category','admission_datetime_filled'],
                      filter_string='end_date > lmp AND start_date < admission_datetime_filled',
                      omit_columns=medication_orders_default_columns_to_omit(),
                      add_cc_columns=[]).filter(F.col('order_class')!='Historical Med').drop(*['order_description', 'order_class', 'order_set', 'order_status', 'order_priority', 'requested_instant_utc', 'authorizing_prov_id', 'department_id', 'controlled', 'recorded_datetime', 'due_datetime', 'scheduled_on_datetime', 'scheduled_for_datetime']).distinct()



mom_meds2 = get_medication_orders(cohort_df=mom,
                      include_cohort_columns=['patient_id', 'gestational_days', 'lmp', 'ob_delivery_delivery_date', 'SGA', 'LBW', 'vLBW', 'preterm_category','admission_datetime_filled'],
                      filter_string='end_date > lmp AND start_date < date_sub(ob_delivery_delivery_date,2)',
                      omit_columns=medication_orders_default_columns_to_omit(),
                      add_cc_columns=[]).filter(F.col('order_class')!='Historical Med').drop(*['order_description', 'order_class', 'order_set', 'order_status', 'order_priority', 'requested_instant_utc', 'authorizing_prov_id', 'department_id', 'controlled', 'recorded_datetime', 'due_datetime', 'scheduled_on_datetime', 'scheduled_for_datetime']).distinct()


mom_meds.createOrReplaceTempView("med1")
mom_meds2.createOrReplaceTempView("med2")


# ----------------------------------------------------------------------------

# 4. clean up medication record  


med1 = spark.sql("""
SELECT *
FROM med1
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


med1_ip = med1.filter(F.col('order_mode')=='Inpatient')
med1_op = med1.filter(F.col('order_mode')=='Outpatient')
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

# COMMAND ----------

def get_medinfo_cohort(cohort_df):
  result_df = cohort_df.withColumn('orderstart_lmp', F.datediff(F.col('start_date'), F.col('lmp'))) .\
                        withColumn('orderend_lmp',  F.datediff(F.col('end_date'), F.col('lmp'))) .\
                        withColumn('order_duration', F.datediff(F.col('end_date'), F.col('start_date'))+1) .\
                        withColumn('orderstart_lmp_period', get_exposure_period_udf(F.col('orderstart_lmp'))) .\
                        withColumn('orderstart_lmp_period', get_exposure_period_udf(F.col('orderstart_lmp'))) .\
                        withColumn('exposure0_duration', get_exposure0_duration_udf(F.col('orderstart_lmp'), F.col('orderend_lmp'))) .\
                        withColumn('exposure1_duration', get_exposure1_duration_udf(F.col('orderstart_lmp'), F.col('orderend_lmp'))) .\
                        withColumn('exposure2_duration', get_exposure2_duration_udf(F.col('orderstart_lmp'), F.col('orderend_lmp'))) .\
                        withColumn('exposure3_duration', get_exposure3_duration_udf(F.col('orderstart_lmp'), F.col('orderend_lmp'), F.col('gestational_days'))) .\
                        withColumn('exposure_total_duration', (F.col('exposure1_duration')+F.col('exposure2_duration')+F.col('exposure3_duration'))) .\
                        withColumn('exposure1_status', F.when(F.col('exposure1_duration') > 0,1).otherwise(0)) .\
                        withColumn('exposure2_status', F.when(F.col('exposure2_duration') > 0,1).otherwise(0)) .\
                        withColumn('exposure3_status', F.when(F.col('exposure3_duration') > 0,1).otherwise(0)) .\
                        withColumn('exposure_total_status', F.when(F.col('exposure_total_duration') > 0,1).otherwise(0))
  return result_df
med1_ip = get_medinfo_cohort(med1_ip)
med1_op = get_medinfo_cohort(med1_op)
med1 = get_medinfo_cohort(med1)
med2_ip = get_medinfo_cohort(med2_ip)
med2_op = get_medinfo_cohort(med2_op)
med2 = get_medinfo_cohort(med2)


from pyspark.sql.functions import arrays_zip, col, explode
med_df_dict = {'med1' : med1,
'med1_op' : med1_op,
'med1_ip' : med1_ip, 
'med2' : med2,
'med2_op' : med2_op,
'med2_ip' : med2_ip
}
new_med_df_dict = {}
for k in med_df_dict.keys():
  old_df = med_df_dict[k]
  new_df = old_df.withColumn("new", F.arrays_zip("medication_description", "term_type"))\
       .withColumn("new", F.explode("new"))\
       .select('pat_id','instance', 'medication_id', F.col('new.medication_description').alias('medication_description'), F.col('new.term_type').alias('term_type'),'rxnorm_code','medication_id','preterm_category', 'LBW', 'SGA','gestational_days','lmp','start_date','end_date','name','short_name','exposure1_duration', 'exposure2_duration', 'exposure3_duration','exposure_total_duration', 'exposure1_status', 'exposure2_status', 'exposure3_status','exposure_total_status').filter(F.col('term_type') == "Ingredient").where(F.col("medication_description").isNotNull()).filter(F.col('term_type') == "Ingredient").distinct()
  new_med_df_dict[k] = new_df 


agg_med_df_dict = {}
partition_columns = ['pat_id', 'instance', 'lmp', 'gestational_days', 'medication_description','medication_id','preterm_category', 'LBW', 'SGA']
agg_cols = {'exposure_total_duration':'sum',
           'exposure1_duration':'sum',
           'exposure2_duration':'sum',
           'exposure3_duration':'sum',
           'exposure_total_status' :'max',
           'exposure1_status':'max',
           'exposure2_status':'max',
           'exposure3_status':'max'}
for k in new_med_df_dict.keys():
  df = new_med_df_dict[k]
  agg_df = aggregate_data(df ,
                          partition_columns = partition_columns, 
                          aggregation_columns = agg_cols)
  agg_df = agg_df.withColumn('medication_description', F.regexp_replace('medication_description', ' ', '_')).withColumn('medication_description', F.regexp_replace('medication_description', '[-,;{}()\n\t=.]', '_'))
  agg_df2 = agg_df.drop( 'LBW', 'SGA', 'preterm_category')
  agg_med_df_dict[k] = agg_df2
  write_data_frame_to_sandbox(agg_df2, "yh_maternity_{0}".format(k), sandbox_db='rdp_phi_sandbox', replace=True)

# ----------------------------------------------------------------------------
# 5. save medication record 

# pivot medication dataframe. rows of individual patients and columns of medication ingredient 
med_df_list = new_med_df_dict.keys()
read_med_df_dict = {}
for m in med_df_list:
  med_df = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_{0}".format(m))
  mom_med_df = mom.join(med_df,['pat_id', 'instance', 'lmp', 'gestational_days'], 'left')
  mom_pivoted_total_df = mom_med_df.groupBy(*mom.columns).pivot('medication_description').max("exposure_total_status_max").drop('Vortioxetine').fillna(0)
  write_data_frame_to_sandbox(mom_pivoted_total_df, "yh_maternity_meds_pivoted_total_{0}".format(m), sandbox_db='rdp_phi_sandbox', replace=True)