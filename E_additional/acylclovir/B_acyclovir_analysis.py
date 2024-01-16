# Author: Yeon Mi Hwang  
# Date: 01/15/2024
# Disclaimer : This code was originally written in databricks workspace. 
# Objective of B_acyclovir_analysis.py : To categorize acyclovir medication record and run analysis 
# Workflow of B_acyclovir_analysis.py
## 1. load necessary packages and functions 
## 2. load relevant dataframe 
## 3. categorize acyclovir and valacyclovir record 
## 4. run analysis 




# ----------------------------------------------------------------------------
# 1. load necessary packages and functions 


import CEDA_Tools_v1_1.load_ceda_etl_tools
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import *

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)


# ----------------------------------------------------------------------------
# 2. load relevant dataframes 

mom = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_2022")
meds = spark.sql("SELECT * FROM rdp_phi.medicationorders")
mom_med = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_meds_2022")



acyclovir = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_meds_2022").filter(F.lower(F.col('ORDERDESCRIPTION')).contains('acyclovir')).filter(~F.lower(F.col('ORDERDESCRIPTION')).contains('valacyclovir'))
valacyclovir = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_meds_2022").filter(F.lower(F.col('ORDERDESCRIPTION')).contains('valacyclovir'))


mom_acyclovir = acyclovir.filter('end_date > lmp').filter('start_date < ob_delivery_delivery_date').drop(*["ordering_datetime", "administration_datetime", "recorded_datetime", "action_taken", "is_timely"]).distinct()
mom_valacyclovir = valacyclovir.filter('end_date > lmp').filter('start_date < ob_delivery_delivery_date').drop(*["ordering_datetime", "administration_datetime", "recorded_datetime", "action_taken", "is_timely"]).distinct()

# ----------------------------------------------------------------------------
# 3. categorize acyclovir and valacyclovir record 

def parse_dosage(sig, description):
  import re
  if description is None:
    return None
  elif sig is None:
    if re.search(r"\d+\s{1}[A-Z]+", description, re.IGNORECASE): 
      return re.search(r"\d+\s{1}[A-Z]+", description, re.IGNORECASE).group().split(" ")[0]
  else:
    if re.search(r"(1,000)\s{1}mg", sig, re.IGNORECASE):
      return "1000"
    elif re.search(r"(2,000)\s{1}mg", sig, re.IGNORECASE):
      return "2000"
    elif re.search(r"\d+\s{1}mg", sig, re.IGNORECASE):
      return re.search(r"\d+\s{1}mg", sig, re.IGNORECASE).group().split(" ")[0]
    elif re.search(r"\d+\s{1}[A-Z]+", description, re.IGNORECASE): 
      return re.search(r"\d+\s{1}[A-Z]+", description, re.IGNORECASE).group().split(" ")[0]
    else:
      return None
parse_dosage_udf = F.udf(lambda sig, description: parse_dosage(sig, description), StringType())  


def clean_dosage(dosage):
  if dosage is None:
    return None
  else:
    if (dosage == 1) | (dosage == 2):
      return dosage*1000
    else:
      return dosage
clean_dosage_udf = F.udf(lambda dosage: clean_dosage(dosage), IntegerType())


def onetime_tab_count(sig):
  import re
  if sig is None:
    return 1
  elif re.search(r"\d+\s{1}mg", sig, re.IGNORECASE):
    return 1
  elif (re.search(r"(0.5)|(1/2)|(one-half)", sig, re.IGNORECASE)):
    return 0.5
  elif re.search(r"\d{1}\s{1}cap[a-z]+", sig, re.IGNORECASE):
    return re.search(r"\d{1}\s{1}cap[a-z]+", sig, re.IGNORECASE).group().split(' ')[0]
  elif re.search(r"\d{1}\s{1}tab[a-z]+", sig, re.IGNORECASE):
    return re.search(r"\d{1}\s{1}tab[a-z]+", sig, re.IGNORECASE).group().split(' ')[0]
  elif re.search(r"\d{1}\s{1}cap[a-z]+", sig,re.IGNORECASE):
    return re.search(r"\d{1}\s{1}cap[a-z]+", sig, re.IGNORECASE).group().split(' ')[0]
  elif re.search(r"\d{1}\s{1}tab[a-z]+", sig,re.IGNORECASE):
    return re.search(r"\d{1}\s{1}tab[a-z]+", sig, re.IGNORECASE).group().split(' ')[0]
  elif (re.search(r"one\s{1}cap[a-z]+", sig, re.IGNORECASE)):
    return 1
  elif (re.search(r"one\s{1}tab[a-z]+", sig, re.IGNORECASE)):
    return 1
  elif (re.search(r"two\s{1}cap[a-z]+", sig, re.IGNORECASE)):
    return 2
  elif (re.search(r"two\s{1}tab[a-z]+", sig, re.IGNORECASE)):
    return 2
  elif (re.search(r"three\s{1}cap[a-z]+", sig, re.IGNORECASE)):
    return 3
  elif (re.search(r"three\s{1}tab[a-z]+", sig, re.IGNORECASE)):
    return 3
  elif re.search(r"\d{1}\s{1}po", sig, re.IGNORECASE):
    return re.search(r"\d{1}\s{1}po", sig, re.IGNORECASE).group().split(' ')[0]
  else:
    return 1
onetime_tab_count_udf = F.udf(lambda sig: onetime_tab_count(sig), StringType()) 

def parse_frequency_1(col):
  import re
  if col is None:
    return None
  else:
    if re.search(r"\d+\s{1}times", col, re.IGNORECASE):
      return re.search(r"\d+\s{1}times", col, re.IGNORECASE).group().split(' ')[0]
    elif re.search(r"twice", col, re.IGNORECASE):
      return 2
    elif re.search(r"three\s{1}times", col, re.IGNORECASE):
      return 3
    elif re.search(r"five\s{1}times", col, re.IGNORECASE):
      return 5
    elif re.search(r"(five)\s{1}times", col, re.IGNORECASE):
      return 5
    elif re.search(r"(two)\s{1}times", col, re.IGNORECASE):
      return 2
    elif re.search(r"(three)\s{1}times", col, re.IGNORECASE):
      return 3
    elif re.search(r"q8h", col, re.IGNORECASE):
      return 3
    elif re.search(r"8\s{1}hours", col, re.IGNORECASE):
      return 3
    elif re.search(r"eight", col, re.IGNORECASE):
      return 3
    elif re.search(r"q4h", col, re.IGNORECASE):
      return 5
    elif re.search(r"5\s{1}x", col, re.IGNORECASE):
        return 5
    elif re.search(r"5x", col, re.IGNORECASE):
        return 5
    elif re.search(r"4\s{1}hours", col, re.IGNORECASE):
      return 5
    elif re.search(r"four\s{1}hours", col, re.IGNORECASE):
      return 5
    elif re.search(r"4\s{1}(four)\s{1}hours", col, re.IGNORECASE):
      return 5
    elif re.search(r"q3h", col, re.IGNORECASE):
      return 8
    elif re.search(r"3\s{1}hours", col, re.IGNORECASE):
      return 8
    elif re.search(r"three\s{1}hours", col, re.IGNORECASE):
      return 8
    elif re.search(r"3\s{1}(three)\s{1}hours", col, re.IGNORECASE):
      return 8
    elif re.search(r"q12h", col, re.IGNORECASE):
      return 2
    elif re.search(r"twelve", col, re.IGNORECASE):
      return 2
    elif re.search(r"12\s{1}hours", col, re.IGNORECASE):
      return 2
    elif re.search(r"tid", col, re.IGNORECASE):
      return 3
    elif re.search(r"bid", col, re.IGNORECASE):
      return 2
    elif re.search(r"\Adaily", col, re.IGNORECASE):
      return 1
    elif re.search(r"nightly", col, re.IGNORECASE):
      return 1
    else:
      return None
parse_frequency_1_udf = F.udf(lambda col: parse_frequency_1(col), StringType())  

def parse_frequency_2(col, frequency):
  import re
  if frequency is not None:
    return frequency
  else:
    if col is None:
      return None
    else:
      if re.search(r"\d+\s{1}times", col, re.IGNORECASE):
        return re.search(r"\d+\s{1}times", col, re.IGNORECASE).group().split(' ')[0]
      elif re.search(r"every\s{1}day", col, re.IGNORECASE):
        return 1
      elif re.search(r"twice", col, re.IGNORECASE):
        return 2
      elif re.search(r"three\s{1}times", col, re.IGNORECASE):
        return 3
      elif re.search(r"five\s{1}times", col, re.IGNORECASE):
        return 5
      elif re.search(r"(five)\s{1}times", col, re.IGNORECASE):
        return 5
      elif re.search(r"(two)\s{1}times", col, re.IGNORECASE):
        return 2
      elif re.search(r"(three)\s{1}times", col, re.IGNORECASE):
        return 3
      elif re.search(r"q8h", col, re.IGNORECASE):
        return 3
      elif re.search(r"8\s{1}hours", col, re.IGNORECASE):
        return 3
      elif re.search(r"eight", col, re.IGNORECASE):
        return 3
      elif re.search(r"q4h", col, re.IGNORECASE):
        return 5
      elif re.search(r"4\s{1}hours", col, re.IGNORECASE):
        return 5
      elif re.search(r"four\s{1}hours", col, re.IGNORECASE):
        return 5
      elif re.search(r"5\s{1}x", col, re.IGNORECASE):
        return 5
      elif re.search(r"5x", col, re.IGNORECASE):
        return 5
      elif re.search(r"4\s{1}(four)\s{1}hours", col, re.IGNORECASE):
        return 5
      elif re.search(r"q3h", col, re.IGNORECASE):
        return 8
      elif re.search(r"3\s{1}hours", col, re.IGNORECASE):
        return 8
      elif re.search(r"three\s{1}hours", col, re.IGNORECASE):
        return 8
      elif re.search(r"3\s{1}(three)\s{1}hours", col, re.IGNORECASE):
        return 8
      elif re.search(r"q12h", col, re.IGNORECASE):
        return 2
      elif re.search(r"twelve", col, re.IGNORECASE):
        return 2
      elif re.search(r"12\s{1}hours", col, re.IGNORECASE):
        return 2
      elif re.search(r"tid", col, re.IGNORECASE):
        return 3
      elif re.search(r"bid", col, re.IGNORECASE):
        return 2
      elif re.search(r"\Adaily", col, re.IGNORECASE):
        return 1
      elif re.search(r"nightly", col, re.IGNORECASE):
        return 1
      else:
        return None
parse_frequency_2_udf = F.udf(lambda col, frequency: parse_frequency_2(col, frequency), StringType())  


def parse_duration(sig):
  import re
  if sig is None:
    return None
  else:
    if re.search(r"\d+\s{1}days", sig,  re.IGNORECASE):
      return re.search(r"\d+\s{1}days", sig,  re.IGNORECASE).group().split(' ')[0]
    elif re.search(r"three\s{1}days", sig,  re.IGNORECASE):
      return 3
    elif re.search(r"four\s{1}days", sig,  re.IGNORECASE):
      return 4
    elif re.search(r"five\s{1}days", sig,  re.IGNORECASE):
      return 5
    elif re.search(r"seven\s{1}days", sig,  re.IGNORECASE):
      return 7
    elif re.search(r"ten\s{1}days", sig,  re.IGNORECASE):
      return 10
    else:
      return None
parse_duration_udf = F.udf(lambda sig: parse_duration(sig), StringType())

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

# COMMAND ----------

# mom_acyclovir.columns

# COMMAND ----------

def get_medinfo_cohort(cohort_df):
  result_df = cohort_df.withColumn('dosage2', parse_dosage_udf(F.col('SIG'), F.col('ORDERDESCRIPTION'))) .\
                        withColumn('dosage2',F.col('dosage2').cast('integer')) .\
                        withColumn('dosage2', clean_dosage_udf(F.col('dosage2'))) .\
                        withColumn('onetime_tab', onetime_tab_count_udf(F.col('SIG'))) .\
                        withColumn('onetime_tab',F.col('onetime_tab').cast('float')) .\
                        withColumn('onetime_dosage', F.col('onetime_tab')*F.col('dosage2')) .\
                        withColumn('frequency', parse_frequency_1_udf(F.col('FREQ_NAME'))) .\
                        withColumn('frequency', parse_frequency_2_udf(F.col('SIG'), F.col('frequency'))) .\
                        withColumn('frequency',F.col('frequency').cast('integer')) .\
                        withColumn('parsed_duration', parse_duration_udf(F.col('SIG'))) .\
                        withColumn('duration', F.when(F.col('parsed_duration').isNull(), F.datediff(F.col('end_date'), F.col('start_date'))+1). otherwise(F.col('parsed_duration'))) .\
                        withColumn('parsed_duration',F.col('parsed_duration').cast('integer')) .\
                        withColumn('orderstart_lmp', F.datediff(F.col('start_date'), F.col('lmp'))) .\
                        withColumn('orderend_lmp', F.when(F.col('parsed_duration').isNull(), F.datediff(F.col('end_date'), F.col('lmp'))) .otherwise(F.col('orderstart_lmp')+F.col('parsed_duration'))) .\
                        withColumn('orderstart_lmp_period', get_exposure_period_udf(F.col('orderstart_lmp'))) .\
                        withColumn('orderstart_lmp_period', get_exposure_period_udf(F.col('orderstart_lmp'))) .\
                        withColumn('exposure0_duration', get_exposure0_duration_udf(F.col('orderstart_lmp'), F.col('orderend_lmp'))) .\
                        withColumn('exposure1_duration', get_exposure1_duration_udf(F.col('orderstart_lmp'), F.col('orderend_lmp'))) .\
                        withColumn('exposure2_duration', get_exposure2_duration_udf(F.col('orderstart_lmp'), F.col('orderend_lmp'))) .\
                        withColumn('exposure3_duration', get_exposure3_duration_udf(F.col('orderstart_lmp'), F.col('orderend_lmp'), F.col('gestational_days')))
  return result_df

mom_acyclovir = get_medinfo_cohort(mom_acyclovir)
mom_valacyclovir = get_medinfo_cohort(mom_valacyclovir)

# COMMAND ----------

def get_genital_herpes_a(onetime_dosage): 
  if (onetime_dosage == 200):
    return 1
  elif (onetime_dosage == 400):
    return 1
  else:
    return 0
get_genital_herpes_a_udf = F.udf(lambda onetime_dosage: get_genital_herpes_a(onetime_dosage), IntegerType())  
def get_shingles_a(onetime_dosage, frequency): 
  if (onetime_dosage == 800)&(frequency == 5):
    return 1
  else:
    return 0
get_shingles_a_udf = F.udf(lambda onetime_dosage, frequency: get_shingles_a(onetime_dosage, frequency), IntegerType())  
def get_chicken_pox_a(onetime_dosage, frequency): 
  if (onetime_dosage == 800)&(frequency == 4):
    return 1
  else:
    return 0
get_chicken_pox_a_udf = F.udf(lambda onetime_dosage, frequency: get_chicken_pox_a(onetime_dosage, frequency), IntegerType())  
def get_cold_sore_a(sig, route): 
  if route == 'Topical':
    return 1
  else: 
    if sig is None:
      return 0
    elif re.search(r"oral\s{1}herpes", sig, re.IGNORECASE):
      return 1
    elif re.search(r"cold", sig, re.IGNORECASE):
      return 1
    else:
      return 0
get_cold_sore_a_udf = F.udf(lambda sig, route: get_cold_sore_a(sig, route), IntegerType()) 
def get_genital_herpes_v(onetime_dosage, frequency): 
  if (onetime_dosage == 1000)&(frequency == 2):
    return 1
  elif (onetime_dosage == 500)&(frequency == 2):
    return 1
  elif (onetime_dosage == 1000)&(frequency == 1):
    return 1
  elif (onetime_dosage == 500)&(frequency == 1):
    return 1
  else:
    return 0
get_genital_herpes_v_udf = F.udf(lambda onetime_dosage, frequency: get_genital_herpes_v(onetime_dosage, frequency), IntegerType())  
def get_shingles_v(onetime_dosage, frequency): 
  if (onetime_dosage == 1000)&(frequency == 3):
    return 1
  else:
    return 0
get_shingles_v_udf = F.udf(lambda onetime_dosage, frequency: get_shingles_v(onetime_dosage, frequency), IntegerType())  
def get_chicken_pox_v(onetime_dosage, frequency): 
  if (onetime_dosage == 1000)&(frequency == 3):
    return 1
  else:
    return 0
get_chicken_pox_v_udf = F.udf(lambda onetime_dosage, frequency: get_chicken_pox_v(onetime_dosage, frequency), IntegerType())  
def get_cold_sore_v(onetime_dosage, frequency): 
  if (onetime_dosage == 2000)&(frequency == 2):
      return 1
  else:
    return 0
get_cold_sore_v_udf = F.udf(lambda onetime_dosage, frequency: get_cold_sore_v(onetime_dosage, frequency), IntegerType()) 

# COMMAND ----------

mom_acyclovir = mom_acyclovir.withColumn('a_genital_herpes', get_genital_herpes_a_udf(F.col('onetime_dosage')))
mom_acyclovir = mom_acyclovir.withColumn('a_chicken_pox', get_chicken_pox_a_udf(F.col('onetime_dosage'), F.col('frequency')))
mom_acyclovir = mom_acyclovir.withColumn('a_shingles', get_shingles_a_udf(F.col('onetime_dosage'), F.col('frequency')))
mom_acyclovir = mom_acyclovir.withColumn('a_cold_sore', get_cold_sore_a_udf(F.col('sig'), F.col('route')))
mom_acyclovir = mom_acyclovir.withColumn('a_28wk', F.when(F.col('orderend_lmp') <= 196, 1).otherwise(0))
mom_acyclovir = mom_acyclovir.withColumn('a_32wk', F.when(F.col('orderend_lmp') <= 224, 1).otherwise(0))
mom_acyclovir = mom_acyclovir.withColumn('a_36wk', F.when(F.col('orderend_lmp') <= 252, 1).otherwise(0))
mom_valacyclovir = mom_valacyclovir.withColumn('v_genital_herpes', get_genital_herpes_v_udf(F.col('onetime_dosage'), F.col('frequency')))
mom_valacyclovir = mom_valacyclovir.withColumn('v_chicken_pox', get_chicken_pox_v_udf(F.col('onetime_dosage'), F.col('frequency')))
mom_valacyclovir = mom_valacyclovir.withColumn('v_shingles', get_shingles_v_udf(F.col('onetime_dosage'), F.col('frequency')))
mom_valacyclovir = mom_valacyclovir.withColumn('v_cold_sore', get_cold_sore_v_udf(F.col('onetime_dosage'), F.col('frequency')))
mom_valacyclovir = mom_valacyclovir.withColumn('v_28wk', F.when(F.col('orderend_lmp') <= 196, 1).otherwise(0))
mom_valacyclovir = mom_valacyclovir.withColumn('v_32wk', F.when(F.col('orderend_lmp') <= 224, 1).otherwise(0))
mom_valacyclovir = mom_valacyclovir.withColumn('v_36wk', F.when(F.col('orderend_lmp') <= 252, 1).otherwise(0))

# COMMAND ----------

# mom_acyclovir_agg.count()

# COMMAND ----------

# mom_acyclovir_agg.filter(F.col('a_36wk_max')==1).count()

# COMMAND ----------

partition_columns = ['pat_id', 'instance', 'lmp','ob_delivery_delivery_date', 'gestational_days', 'race_group', 'preterm_category']
a_agg_columns = {
  'exposure0_duration': 'sum',
  'exposure1_duration': 'sum',
  'exposure2_duration': 'sum',
  'exposure3_duration': 'sum',
  'a_genital_herpes' : 'max',  
  'a_chicken_pox' : 'max',  
  'a_shingles' : 'max', 
  'a_cold_sore' : 'max', 
  'a_28wk' : 'max', 
  'a_32wk' : 'max', 
  'a_36wk' : 'max'
}
v_agg_columns = {
  'exposure0_duration': 'sum',
  'exposure1_duration': 'sum',
  'exposure2_duration': 'sum',
  'exposure3_duration': 'sum',
  'v_genital_herpes' : 'max',  
  'v_chicken_pox' : 'max',  
  'v_shingles' : 'max', 
  'v_cold_sore' : 'max', 
  'v_28wk' : 'max', 
  'v_32wk' : 'max', 
  'v_36wk' : 'max'
}
mom_acyclovir_agg = aggregate_data(df = mom_acyclovir, partition_columns = partition_columns, aggregation_columns  =  a_agg_columns)
mom_valacyclovir_agg = aggregate_data(df = mom_valacyclovir, partition_columns = partition_columns, aggregation_columns  =  v_agg_columns)

# COMMAND ----------

mom_acyclovir_agg = mom_acyclovir_agg.withColumnRenamed('exposure0_duration_sum', 'a_exposure0_duration_sum')
mom_acyclovir_agg = mom_acyclovir_agg.withColumnRenamed('exposure1_duration_sum', 'a_exposure1_duration_sum')
mom_acyclovir_agg = mom_acyclovir_agg.withColumnRenamed('exposure2_duration_sum', 'a_exposure2_duration_sum')
mom_acyclovir_agg = mom_acyclovir_agg.withColumnRenamed('exposure3_duration_sum', 'a_exposure3_duration_sum')
mom_valacyclovir_agg = mom_valacyclovir_agg.withColumnRenamed('exposure0_duration_sum', 'v_exposure0_duration_sum')
mom_valacyclovir_agg = mom_valacyclovir_agg.withColumnRenamed('exposure1_duration_sum', 'v_exposure1_duration_sum')
mom_valacyclovir_agg = mom_valacyclovir_agg.withColumnRenamed('exposure2_duration_sum', 'v_exposure2_duration_sum')
mom_valacyclovir_agg = mom_valacyclovir_agg.withColumnRenamed('exposure3_duration_sum', 'v_exposure3_duration_sum')

# COMMAND ----------

mom_acyclovir_agg = mom_acyclovir_agg.withColumn('acyclovir', F.lit(1)).drop('ob_delivery_delivery_date', 'gestational_days', 'race_group', 'preterm_category')
mom_valacyclovir_agg = mom_valacyclovir_agg.withColumn('valacyclovir', F.lit(1)).drop('ob_delivery_delivery_date', 'gestational_days', 'race_group', 'preterm_category')

# COMMAND ----------

mom_valacyclovir_agg.columns

# COMMAND ----------

mom = mom.join(mom_acyclovir_agg, ['pat_id', 'instance','lmp'], 'left').join(mom_valacyclovir_agg, ['pat_id', 'instance', 'lmp'], 'left').fillna(0,[ 'a_exposure0_duration_sum',
 'a_exposure1_duration_sum',
 'a_exposure2_duration_sum',
 'a_exposure3_duration_sum',
 'a_genital_herpes_max',
 'a_chicken_pox_max',
 'a_shingles_max',
 'a_cold_sore_max',
 'acyclovir',
 'v_exposure0_duration_sum',
 'v_exposure1_duration_sum',
 'v_exposure2_duration_sum',
 'v_exposure3_duration_sum',
 'v_genital_herpes_max',
 'v_chicken_pox_max',
 'v_shingles_max',
 'v_cold_sore_max',
 'valacyclovir'])

# COMMAND ----------

# DBTITLE 1,based on exposure date 
from pyspark.sql.functions import isnan
mom = mom.withColumn('acyclovir', F.when(F.col('acyclovir').isNull(), 0).otherwise(F.col('acyclovir')))
mom = mom.withColumn('a_28wk', F.when(F.col('a_28wk_max').isNull(), 0)\
                               .when(F.col('a_28wk_max')==0, None)\
                               .otherwise(F.col('a_28wk_max')))
mom = mom.withColumn('a_32wk', F.when(F.col('a_32wk_max').isNull(), 0)\
                               .when(F.col('a_32wk_max')==0, None)\
                               .otherwise(F.col('a_32wk_max')))
mom = mom.withColumn('a_36wk', F.when(F.col('a_36wk_max').isNull(), 0)\
                               .when(F.col('a_36wk_max')==0, None)\
                               .otherwise(F.col('a_36wk_max')))
mom = mom.withColumn('valacyclovir', F.when(F.col('valacyclovir').isNull(), 0).otherwise(F.col('valacyclovir')))
mom = mom.withColumn('v_28wk', F.when(F.col('v_28wk_max').isNull(), 0)\
                                .when(F.col('v_28wk_max')==0, None)\
                                .otherwise(F.col('v_28wk_max')))
mom = mom.withColumn('v_32wk', F.when(F.col('v_32wk_max').isNull(), 0)\
                                .when(F.col('v_32wk_max')==0, None)\
                                .otherwise(F.col('v_32wk_max')))
mom = mom.withColumn('v_36wk', F.when(F.col('v_36wk_max').isNull(), 0)\
                                .when(F.col('v_36wk_max')==0, None)\
                                .otherwise(F.col('v_36wk_max')))

# COMMAND ----------

def acyclovir_valacyclovir(a, v): 
  if (a==1)&(v==1):
    return 'both_exposed'
  elif (a==1):
    return 'acyclovir_only'
  elif (v==1):
    return 'valacyclovir_only'
  else:
    return 'not exposed'
acyclovir_valacyclovir_udf = F.udf(lambda a, v: acyclovir_valacyclovir(a, v), StringType()) 
mom = mom.withColumn('acyclovir_valacyclovir', acyclovir_valacyclovir_udf(F.col('acyclovir'), F.col('valacyclovir')))
mom = mom.withColumn('acyclovir_valacyclovir_36wk', acyclovir_valacyclovir_udf(F.col('a_36wk'), F.col('v_36wk')))

# COMMAND ----------

mom_hsv_indication = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_hsv_indication_new")

# COMMAND ----------

mom_indication = mom.join(mom_hsv_indication, ['patient_id', 'lmp', 'ob_delivery_delivery_date'], 'left').fillna(0,['shingles', 'genital_herpes', 'chicken_pox', 'cold_sore', 'herpes_simplex', 'hsv1', 'hsv2', 'primary_gh', 'recurrent_gh'])

# COMMAND ----------

table_name = 'yh_maternity_hsv_indication_joined_new'
write_data_frame_to_sandbox(mom_indication, table_name, sandbox_db='rdp_phi_sandbox', replace=True)

# COMMAND ----------

mom_indication = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_hsv_indication_joined_new")

# ----------------------------------------------------------------------------
# 4. run analysis 


from pyspark.sql.functions import col, create_map, lit
from itertools import chain
mapping = {'SGA': 1, 'nonSGA': 0, 'LBW': 1, 'nonLBW': 0, 'preterm': 1, 'term':0}
mapping_expr = create_map([F.lit(x) for x in chain(*mapping.items())])

def convert_df(df):
  df = df.withColumn('delivery_year', F.year(F.col('ob_delivery_delivery_date')))
  df = df.withColumn('alcohol_user', F.col('alcohol_user').cast(FloatType()))
  df = df.withColumn('smoker', F.col('smoker').cast(FloatType()))
  df = df.withColumn('illegal_drug_user', F.col('illegal_drug_user').cast(FloatType()))
  df = df.withColumn('pregravid_bmi', F.col('pregravid_bmi').cast(FloatType()))
  df = df.withColumn('Preterm_history', F.col('Preterm_history').cast(FloatType()))
  df = df.withColumn('Parity', F.col('Parity').cast(FloatType()))
  df = df.withColumn('Gravidity', F.col('Gravidity').cast(FloatType()))
  df = df.withColumn('PTB_category', mapping_expr.getItem(F.col("preterm_category")))
  df = df.withColumn('SGA_category', mapping_expr.getItem(F.col("SGA")))
  df = df.withColumn('LBW_category', mapping_expr.getItem(F.col("LBW")))
  return df
mom_indication = convert_df(mom_indication)

# COMMAND ----------

def format_race_group(i):
  if i is None:
    return('Other')
  if i == 'Multirace':
    return('Other')
  if i == 'American Indian or Alaska Native':
    return('Other')
  if i == 'Native Hawaiian or Other Pacific Islander':
    return('Other')
  else:
    return i
format_race_group_udf = F.udf(lambda i: format_race_group(i), StringType())  

def format_ethnic_group(i):
  if i is None:
    return 'Unknown'
  if i == 'American' or i == 'Samoan':
    return 'Not Hispanic or Latino'
  elif i == 'Filipino' or i == 'Hmong':
    return 'Not Hispanic or Latino'
  elif i == 'Sudanese':
    return 'Not Hispanic or Latino'
  if i == 'Patient Refused' or i == 'None':
    return 'Unknown'
  return i
format_ethnic_group_udf = F.udf(lambda i: format_ethnic_group(i), StringType())  

def format_parity(parity):
  if parity is None:
    return 0
  if math.isnan(parity):
    return 0
  parity = int(parity)
  if parity == 0 or parity == 1:
    return 0
  if parity > 1 and parity < 5:
    return 1
  if parity >= 5:
    return 2
  return 0
format_parity_udf = F.udf(lambda parity: format_parity(parity), IntegerType())    

def format_gravidity(gravidity):
  if gravidity is None:
    return -1
  if math.isnan(gravidity):
    return -1
  gravidity = int(gravidity)
  if gravidity == 0 or gravidity == 1:
    return 0
  elif gravidity > 1 and gravidity < 6:
    return 1
  elif gravidity >= 6:
    return 2
  else:
    return -1
format_gravidity_udf = F.udf(lambda gravidity: format_gravidity(gravidity), IntegerType())    
  
def format_preterm_history(preterm_history, parity):
  import math
  if parity is None:
    return -1
  if math.isnan(parity):
    return -1
  else:
    if preterm_history is None:
      return 0
    if math.isnan(preterm_history):
      return 0
    preterm_history = int(preterm_history)
    if preterm_history == 0 :
      return 0
    else:
      return 1
format_preterm_history_udf = F.udf(lambda preterm_history, parity: format_preterm_history(preterm_history, parity), IntegerType())

def encode_delivery_method(i):
  '''
  0 = Vaginal
  1 = C-Section
  -1 = Unknown
  '''
  list_vaginal = ['Vaginal, Spontaneous',
       'Vaginal, Vacuum (Extractor)',
       'Vaginal, Forceps', 'Vaginal < 20 weeks',
       'Vaginal, Breech', 'VBAC, Spontaneous',
       'Vaginal Birth after Cesarean Section',
       'Spontaneous Abortion']
  list_c_section = ['C-Section, Low Transverse',
       'C-Section, Low Vertical',
       'C-Section, Classical',
       'C-Section, Unspecified']
  if i in list_vaginal:
    return(0)
  elif i in list_c_section:
    return(1)
  else:
    return(-1)
encode_delivery_method_udf = F.udf(lambda i: encode_delivery_method(i), IntegerType())

def encode_bmi(bmi):
  if bmi is None or math.isnan(bmi):
    return -1
  bmi = int(bmi)
  if bmi >= 15 and bmi < 18.5:
    return 0
  elif bmi < 25:
    return 1
  elif bmi < 30:
    return 2
  elif bmi < 35:
    return 3
  elif bmi < 40:
    return 4
  else:
    return -1
encode_bmi_udf = F.udf(lambda bmi: encode_bmi(bmi), IntegerType())

def encode_ruca(ruca):
  if ruca is None:
    return -1
  elif ruca == 'Rural':
    return 0
  elif ruca == 'SmallTown':
    return 1
  elif ruca == 'Micropolitan':
    return 2
  elif ruca == 'Metropolitan':
    return 3
  else:
    return -1
encode_ruca_udf = F.udf(lambda ruca: encode_ruca(ruca), IntegerType())
def encode_age(age):
  if age < 25:
    return 0
  elif age < 30:
    return 1
  elif age < 35:
    return 2
  elif age < 40:
    return 3
  elif age < 45:
    return 4
  else:
    return -1
encode_age_udf = F.udf(lambda age: encode_age(age), IntegerType())

def encode_year(year):
  if year > 2019:
    return '2020_2022'
  elif year > 2016:
    return '2017_2019'
  elif year > 2012:
    return '2013_2016'
  else:
    return None
encode_year_udf = F.udf(lambda year: encode_year(year), StringType())

def translate_udf(dictionary):
    return F.udf(lambda col: dictionary.get(col),
               StringType())


def format_spark_dataframe_for_psm(df, select_columns, med_columns, result_columns):
  dict_white = {'White or Caucasian': 1, 'Unknown': 0, 'Asian': 0, 'Multiracial': 0, 'Other': 0, 'Black or African American': 0, 'Native Hawaiian or Other Pacific Islander': 0, 'American Indian or Alaska Native': 0}
  dict_asian = {'White or Caucasian': 0, 'Unknown': 0, 'Asian': 1, 'Multiracial': 0, 'Other': 0, 'Black or African American': 0, 'Native Hawaiian or Other Pacific Islander': 0, 'American Indian or Alaska Native': 0}
  dict_other = {'White or Caucasian': 0, 'Unknown': 0, 'Asian': 0, 'Multiracial': 0, 'Other': 1, 'Black or African American': 0, 'Native Hawaiian or Other Pacific Islander': 0, 'American Indian or Alaska Native': 0}
  dict_black = {'White or Caucasian': 0, 'Unknown': 0, 'Asian': 0, 'Multiracial': 0, 'Other': 0, 'Black or African American': 1, 'Native Hawaiian or Other Pacific Islander': 0, 'American Indian or Alaska Native': 0}
  dict_ethnic_groups = {None:-1, 'Unknown_NotReported': -1, 'Hispanic_Latino': 1, 'Not_Hispanic_Latino': 0}
  dict_fetal_sex = {None: -1, 'Male': 1, 'Female': 0, 'Other': -1, 'Unknown': -1}
  dict_commercial_insurance = {'Medicaid': 0, 'Medicare': 0, 'Uninsured-Self-Pay': 0, None: 0, 'Other': 0, 'Commercial': 1}
  dict_governmental_insurance = {'Medicaid': 1, 'Medicare': 0, 'Uninsured-Self-Pay': 0, None: 0, 'Other': 0, 'Commercial': 0}
  dict_20202022 = {'2020_2022': 1, '2017_2019': 0, '2013_2016': 0, None: 0}
  dict_20172019 = {'2020_2022': 0, '2017_2019': 1, '2013_2016': 0, None: 0}
  dict_20132016 = {'2020_2022': 0, '2017_2019': 0, '2013_2016': 1, None: 0}
  intact_columns = med_columns + result_columns
  selected_df = df.select(*select_columns)
  selected_df = selected_df.withColumn('race_group',format_race_group_udf(F.col('race_group')))
  selected_df = selected_df.withColumn('Preterm_history',format_preterm_history_udf(F.col('Preterm_history'),F.col('Parity')))
  selected_df = selected_df.withColumn('Parity',format_parity_udf(F.col('Parity')))
  selected_df = selected_df.withColumn('Gravidity',format_gravidity_udf(F.col('Gravidity')))
  selected_df = selected_df.withColumn('pregravid_bmi',encode_bmi_udf(F.col('pregravid_bmi')))
  selected_df = selected_df.withColumn('age_at_start_dt',encode_age_udf(F.col('age_at_start_dt')))
  selected_df = selected_df.withColumn('delivery_year',encode_year_udf(F.col('delivery_year')))
  selected_df = selected_df.withColumn('year_20202022',translate_udf(dict_20202022)(F.col('delivery_year')))
  selected_df = selected_df.withColumn('year_20172019',translate_udf(dict_20172019)(F.col('delivery_year')))
  selected_df = selected_df.withColumn('year_20132016',translate_udf(dict_20132016)(F.col('delivery_year')))
  selected_df = selected_df.withColumn('race_white',translate_udf(dict_white)(F.col('race_group')))
  selected_df = selected_df.withColumn('race_asian',translate_udf(dict_asian)(F.col('race_group')))
  selected_df = selected_df.withColumn('race_other',translate_udf(dict_other)(F.col('race_group')))
  selected_df = selected_df.withColumn('race_black',translate_udf(dict_black)(F.col('race_group')))
  selected_df = selected_df.withColumn('ethnic_group',translate_udf(dict_ethnic_groups)(F.col('ethnic_group')))
  selected_df = selected_df.withColumn('ob_hx_infant_sex',translate_udf(dict_fetal_sex)(F.col('ob_hx_infant_sex')))
  selected_df = selected_df.withColumn('commercial_insurance',translate_udf(dict_commercial_insurance)(F.col('insurance')))
  selected_df = selected_df.drop(*['gestational_days', 'insurance', 'race_group', 'delivery_year'])
  covariate_columns = list(set(selected_df.columns) - set(intact_columns))
  return_df = selected_df.fillna(-1, subset=covariate_columns)
  print('Columns used for matching:')
  for col in covariate_columns:
    print(col)
  return(return_df, covariate_columns)


def get_propensity_score(T_col, X_cols, df):
  from sklearn.linear_model import LogisticRegression
  ps_model = LogisticRegression(C=1e6, max_iter=10000, solver='lbfgs').fit(df[X_cols], df[T_col])
  data_ps = df.assign(propensity_score=ps_model.predict_proba(df[X_cols])[:, 1])
  return (data_ps)

# COMMAND ----------

# mom_indication.columns

# COMMAND ----------

basic_columns = ['delivery_year','ob_hx_infant_sex', 'ethnic_group', 'age_at_start_dt', 'race_group', 'Parity', 'Gravidity', 'insurance', 'pregravid_bmi', 'Preterm_history']
basic_binary_columns = ['smoker', 'illegal_drug_user', 'alcohol_user']
columns_including = ['PTB_category', 'SGA_category', 'LBW_category', 'gestational_days',  'shingles',
 'genital_herpes',
 'chicken_pox',
 'cold_sore',
 'herpes_simplex',
 'hsv1',
 'hsv2',
 'primary_gh',
 'recurrent_gh']
med_list = [
 'a_genital_herpes_max',
 'a_chicken_pox_max',
 'a_shingles_max',
 'a_cold_sore_max',
 'a_28wk_max',
 'a_32wk_max',
 'a_36wk_max',
 'acyclovir',
 'v_genital_herpes_max',
 'v_chicken_pox_max',
 'v_shingles_max',
 'v_cold_sore_max',
 'v_28wk_max',
 'v_32wk_max',
 'v_36wk_max',
 'valacyclovir',
 'a_28wk',
 'a_32wk',
 'a_36wk',
 'v_28wk',
 'v_32wk',
 'v_36wk',
 'acyclovir_valacyclovir',
  'acyclovir_valacyclovir_36wk']

# COMMAND ----------

select_columns = list(set(basic_columns + basic_binary_columns + columns_including + med_list))

# COMMAND ----------

# ps_cohort, full_covariate = format_spark_dataframe_for_psm(mom_indication, select_columns, med_list, columns_including)

# COMMAND ----------


def iterative_impute_missing_data(df):
  from sklearn.experimental import enable_iterative_imputer
  from sklearn.impute import IterativeImputer
  imputer = IterativeImputer(max_iter = 10, random_state=42)
  df_return = pd.DataFrame(imputer.fit_transform(df), columns=df.columns)
  return(df_return)


def get_propensity_score(T_col, X_cols, df):
  from sklearn.linear_model import LogisticRegression
  ps_model = LogisticRegression(C=1e6, max_iter=10000, solver='lbfgs').fit(df[X_cols], df[T_col])
  data_ps = df.assign(propensity_score=ps_model.predict_proba(df[X_cols])[:, 1])
  return (data_ps)


def unadjusted(df, x, outcome):
  import statsmodels.formula.api as smf
  model= smf.logit(formula="{0} ~ {1}".format(outcome, x), data= df).fit()
  model_odds = pd.DataFrame(np.exp(model.params), columns= ['OR'])
  model_odds['z-value']= model.pvalues
  model_odds[['2.5%', '97.5%']] = np.exp(model.conf_int())
  model_odds
  return model_odds


def propensity_score_adjustment(df, x , outcome):
  import statsmodels.formula.api as smf
  model= smf.logit(formula="{0} ~ {1} + propensity_score".format(outcome, x), data= df).fit()
  model_odds = pd.DataFrame(np.exp(model.params), columns= ['OR'])
  model_odds['z-value']= model.pvalues
  model_odds[['2.5%', '97.5%']] = np.exp(model.conf_int())
  model_odds
  return model_odds 

# COMMAND ----------

# MAGIC %md 
# MAGIC # Plan of Analysis 
# MAGIC ## acyclovir to unexposed, valacyclovir to unexposed 
# MAGIC 1. acyclovir, valacyclovir exposure to total population : Multivariate regression & PSM & scikit learn 
# MAGIC 2. acyclovir, valacyclovir <36 weeks exposure to total population : Multivariate regression & PSM & scikit learn 
# MAGIC 3. acyclovir, valacyclovir - genital herpes indication to total population 
# MAGIC 4. acyclovir, valacyclovir - genital herpes + cold sore medication exposure to total population 
# MAGIC 5. acyclovir, valacyclovir - valid bmi 
# MAGIC ## acyclovir vs valacyclovir 
# MAGIC 6. acyclovir, valacyclovir comparison : Multivariate regression & PSM & scikit learn
# MAGIC 7. acyclovir, valacyclovir comparison : genital herpes : Multivariate regression & PSM & Scikit learn
# MAGIC 8. acyclovir, valacyclovir comparison : bmi 

# COMMAND ----------

basic_columns = ['delivery_year','ob_hx_infant_sex', 'ethnic_group', 'age_at_start_dt', 'race_group', 'Parity', 'Gravidity', 'insurance', 'pregravid_bmi', 'Preterm_history', 'gestational_days']
basic_binary_columns = ['smoker', 'illegal_drug_user', 'alcohol_user']
med_related_columns = ['shingles', 'genital_herpes', 'chicken_pox', 'cold_sore', 'herpes_simplex', 'hsv1', 'hsv2', 'primary_gh', 'recurrent_gh','a_exposure0_duration_sum',
 'a_exposure1_duration_sum',
 'a_exposure2_duration_sum',
 'a_exposure3_duration_sum',
 'a_genital_herpes_max',
 'a_chicken_pox_max',
 'a_shingles_max',
 'a_cold_sore_max',
 'a_28wk',
 'a_32wk',
 'a_36wk',
 'acyclovir',
 'v_exposure0_duration_sum',
 'v_exposure1_duration_sum',
 'v_exposure2_duration_sum',
 'v_exposure3_duration_sum',
 'v_genital_herpes_max',
 'v_chicken_pox_max',
 'v_shingles_max',
 'v_cold_sore_max',
 'v_28wk',
 'v_32wk',
 'v_36wk',
 'valacyclovir',
 'acyclovir_valacyclovir', 
 'acyclovir_valacyclovir_36wk']
result_columns = ['PTB_category']

# COMMAND ----------

# ps_cohort, full_covariate = format_spark_dataframe_for_psm(mom_indication, select_columns, med_list, columns_including)

# COMMAND ----------

def get_propensity_score(T_col, X_cols, df):
  from sklearn.linear_model import LogisticRegression
  ps_model = LogisticRegression(C=1e6, max_iter=10000, solver='lbfgs').fit(df[X_cols], df[T_col])
  data_ps = df.assign(propensity_score=ps_model.predict_proba(df[X_cols])[:, 1])
  return (data_ps)

def propensity_score_matching(df, x):
  treatment = df[x] 
  mask = treatment == 1
  pscore = df['propensity_score']
  pos_pscore = np.asarray(pscore[mask])
  neg_pscore = np.asarray(pscore[~mask])
  print('treatment count:', pos_pscore.shape)
  print('control count:', neg_pscore.shape)
  from sklearn.neighbors import NearestNeighbors
  if len(neg_pscore) > len(pos_pscore):
    knn = NearestNeighbors(metric='euclidean')
    knn.fit(neg_pscore.reshape(-1, 1))
    distances, indices = knn.kneighbors(pos_pscore.reshape(-1, 1))
    df_pos = df[mask]
    df_neg = df[~mask].iloc[indices[:, 0]]
  else: 
    knn = NearestNeighbors(metric='euclidean')
    knn.fit(pos_pscore.reshape(-1, 1))
    distances, indices = knn.kneighbors(neg_pscore.reshape(-1, 1))
    df_pos = df[mask].iloc[indices[:, 0]] 
    df_neg = df[~mask]
  df_matched = pd.concat([df_pos, df_neg], axis=0)
  print (df_matched.shape)
  return (indices, df_matched)


# COMMAND ----------

# MAGIC %md 
# MAGIC ###  acyclovir to total population 
# MAGIC 1. acyclovir exposure to total population : Multivariate regression & PSM  
# MAGIC 2. acyclovir <36 weeks exposure to total population : Multivariate regression & PSM 
# MAGIC 3. acyclovir - genital herpes indication to total population 
# MAGIC 4. acyclovir - valid bmi 

# COMMAND ----------

psm_cohort_pd = ps_cohort.toPandas()

# COMMAND ----------

# import numpy as np 
# ps_acyclovir = get_propensity_score('acyclovir', full_covariate, psm_cohort_pd)
# ps_valacyclovir = get_propensity_score('valacyclovir', full_covariate, psm_cohort_pd)
# indicies, matched_acyclovir = propensity_score_matching(ps_acyclovir , 'acyclovir')
# indicies, matched_valacyclovir = propensity_score_matching(ps_valacyclovir , 'valacyclovir')

# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC #### 1. acyclovir, valacyclovir exposure to total population : Multivariate regression & PSM & scikit learn 

# COMMAND ----------

# DBTITLE 1,propensity score matching result - acyclovir
# import statsmodels.formula.api as smf
# model= smf.logit(formula="{0} ~ {1}".format('PTB_category', 'acyclovir'), data= matched_acyclovir).fit(method = 'lbfgs')
# model_odds = pd.DataFrame(np.round(np.exp(model.params),2), columns= ['OR'])
# model_odds['z-value']= np.round(model.pvalues,2)
# model_odds[['2.5%', '97.5%']] = np.round(np.exp(model.conf_int()),2)
# model_odds

# COMMAND ----------


# model= smf.logit(formula="{0} ~ {1}".format('PTB_category', 'valacyclovir'), data= matched_valacyclovir).fit(method = 'lbfgs')
# model_odds = pd.DataFrame(np.round(np.exp(model.params),2), columns= ['OR'])
# model_odds['z-value']= np.round(model.pvalues,2)
# model_odds[['2.5%', '97.5%']] = np.round(np.exp(model.conf_int()),2)
# model_odds

# COMMAND ----------

# MAGIC %md 
# MAGIC #### 2. acyclovir, valacyclovir <36 weeks exposure to total population : Multivariate regression & PSM

# COMMAND ----------



# COMMAND ----------

a_36 = psm_cohort_pd[psm_cohort_pd.a_36wk.notnull()]
v_36 = psm_cohort_pd[psm_cohort_pd.v_36wk.notnull()]

# COMMAND ----------

# import numpy as np
# ps_a36 = get_propensity_score('a_36wk', full_covariate, a_36)
# ps_v36 = get_propensity_score('v_36wk', full_covariate, v_36)
# indicies, matched_acyclovir_36wk = propensity_score_matching(ps_a36 , 'a_36wk')
# indicies, matched_valacyclovir_36wk = propensity_score_matching(ps_v36 , 'v_36wk')

# COMMAND ----------

# import statsmodels.formula.api as smf
# model= smf.logit(formula="{0} ~ {1}".format('PTB_category', 'a_36wk'), data= matched_acyclovir_36wk).fit(method = 'lbfgs')
# model_odds = pd.DataFrame(np.round(np.exp(model.params),2), columns= ['OR'])
# model_odds['z-value']= np.round(model.pvalues,2)
# model_odds[['2.5%', '97.5%']] = np.round(np.exp(model.conf_int()),2)
# model_odds

# COMMAND ----------


# model= smf.logit(formula="{0} ~ {1}".format('PTB_category', 'v_36wk'), data= matched_valacyclovir_36wk).fit(method = 'lbfgs')
# model_odds = pd.DataFrame(np.round(np.exp(model.params),2), columns= ['OR'])
# model_odds['z-value']= np.round(model.pvalues,2)
# model_odds[['2.5%', '97.5%']] = np.round(np.exp(model.conf_int()),2)
# model_odds

# COMMAND ----------

# MAGIC %md 
# MAGIC #### 3. acyclovir, valacyclovir genital herpes : Multivariate regression & PSM

# COMMAND ----------

mom_a_gh= a_36[(a_36.genital_herpes==1)]
mom_v_gh= v_36[(v_36.genital_herpes==1)]

# COMMAND ----------

# mom_a_gh.shape

# COMMAND ----------

# mom_v_gh.shape

# COMMAND ----------

def propensity_score_adjustment(df, x , outcome):
  import statsmodels.formula.api as smf
  model= smf.logit(formula="{0} ~ {1} + propensity_score".format(outcome, x), data= df).fit()
  model_odds = pd.DataFrame(np.exp(model.params), columns= ['OR'])
  model_odds['z-value']= model.pvalues
  model_odds[['2.5%', '97.5%']] = np.exp(model.conf_int())
  model_odds
  return model_odds 

# COMMAND ----------

ps_a_gh = get_propensity_score('a_36wk', full_covariate, mom_a_gh)
ps_v_gh = get_propensity_score('v_36wk', full_covariate, mom_v_gh)


# COMMAND ----------

# acyclovir_model_odds = propensity_score_adjustment(ps_a_gh, 'a_36wk', 'PTB_category')
# acyclovir_model_odds

# COMMAND ----------

# valacyclovir_model_odds = propensity_score_adjustment(ps_v_gh, 'v_36wk', 'PTB_category')
# valacyclovir_model_odds

# COMMAND ----------

# MAGIC %md
# MAGIC ### acyclovir vs valacyclovir 

# COMMAND ----------



# COMMAND ----------

a_v_36 = psm_cohort_pd[psm_cohort_pd.a_36wk.notnull()&psm_cohort_pd.v_36wk.notnull()]

# COMMAND ----------

mom_v_a = a_v_36[(a_v_36.acyclovir_valacyclovir_36wk=='acyclovir_only')|(a_v_36.acyclovir_valacyclovir_36wk=='valacyclovir_only')]

# COMMAND ----------

# mom_v_a.shape

# COMMAND ----------

mom_v_a['acyclovir_36wk_only'] = np.where(mom_v_a['acyclovir_valacyclovir_36wk']== 'acyclovir_only', 1, 0)

# COMMAND ----------

# ps_a_va = get_propensity_score('acyclovir_36wk_only', full_covariate, mom_v_a)
# acyclovir_model_odds = propensity_score_adjustment(ps_a_va, 'acyclovir_36wk_only', 'PTB_category')
# acyclovir_model_odds

# COMMAND ----------

import statsmodels.formula.api as smf
model= smf.logit(formula="{0} ~ {1}".format('PTB_category', 'acyclovir'), data= matched_a_va).fit(method = 'bfgs', maxiter =10000)
model_odds = pd.DataFrame(np.round(np.exp(model.params),2), columns= ['OR'])
model_odds['z-value']= np.round(model.pvalues,2)
model_odds[['2.5%', '97.5%']] = np.round(np.exp(model.conf_int()),2)
model_odds

# COMMAND ----------

model_odds

# COMMAND ----------

mom_a = mom_acyclovir.toPandas()

# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt
sns.displot(mom_a, x="orderstart_lmp")
plt.xlim(-180, 300)

# COMMAND ----------

from collections import Counter
data = Counter(mom_a.orderstart_lmp)
data.most_common()   # Returns all unique items and their counts
data.most_common(1)  # Returns the highest occurring item

# COMMAND ----------

