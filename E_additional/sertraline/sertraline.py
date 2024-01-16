# Author: Yeon Mi Hwang  
# Date: 01/15/2024
# Disclaimer : This code was originally written in databricks workspace. 
# Objective of sertraline.py : To get indication and analysis for sertraline 
# Workflow of sertraline.py
## 1. load necessary packages and functions 
## 2. load dataframes 
## 3. get indication 
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
# 2. load dataframe
total = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_meds_diagnoses_op_2022")
mom_med = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_meds_2022")


# ----------------------------------------------------------------------------
# 3. get indication 

depression = total.filter(F.col('prepregnancy_depression')==1)


# COMMAND ----------

select_columns = ['instance',
 'pat_id',
 'lmp',
 'ob_delivery_delivery_date',
 'patient_id',
 'episode_id',
 'birth_date',
 'ethnic_group',
 'race',
 'type',
 'status',
 'working_delivery_date',
 'induction_datetime',
 'number_of_fetuses',
 'ob_sticky_note_text',
 'pregravid_bmi',
 'pregravid_weight',
 'age_at_start_dt',
 'ob_delivery_episode_type',
 'ob_delivery_delivery_csn_mom',
 'ob_delivery_labor_onset_date',
 'ob_hx_clinical_free_txt',
 'child_type',
 'child_ob_delivery_episode_type',
 'delivery_delivery_method',
 'child_ob_delivery_delivery_date',
 'ob_delivery_birth_csn_baby',
 'ob_delivery_record_baby_id',
 'child_ob_delivery_labor_onset_date',
 'delivery_birth_comments',
 'delivery_infant_birth_length_in',
 'delivery_infant_birth_weight_oz',
 'ob_delivery_department',
 'ob_history_last_known_living_status',
 'ob_hx_gestational_age_days',
 'ob_hx_delivery_site',
 'ob_hx_delivery_site_comment',
 'ob_hx_infant_sex',
 'ob_hx_living_status',
 'ob_hx_outcome',
 'gestational_days',
 'preterm_5category',
 'preterm_category',
 'sb_delivery_living_status',
 'pregnancy_outcome',
 'age_group',
 'BMI_category',
 'race_group',
 'delivery_method',
 'delivery_year',
 'first_contact',
 'last_contact',
 'insurance',
 'smoker',
 'illegal_drug_user',
 'alcohol_user',
 'GPAL_max',
 'Gravidity',
 'Parity',
 'Preterm_history',
 'insurance_group',
 'parity_group',
 'fetal_growth_percentile',
 'SGA',
 'LBW',
 'vLBW',
 'admission_datetime_max']


# COMMAND ----------

cc_columns = ['antidepressant', 'antidepressant_N06AA', 'antidepressant_N06AB', 'antidepressant_N06AF', 'antidepressant_N06AG', 'antidepressant_N06AX', 'antidepressant_SNRI', 'antidepressant_SRA', 'citalopram', 'escitalopram', 'fluoxetine', 'paroxetine', 'sertraline', 'fluvoxamine']

# Specify columns to keep in the cohort df
filter_string = \
"""
date_sub(lmp, 180) <= start_date AND start_date <= ob_delivery_delivery_date
"""
# Get medication records (add T/F columns indicating antidepressant medication orders)
med_depression = get_medication_orders(
  cohort_df= depression,
  include_cohort_columns=select_columns,
  filter_string = filter_string,
  add_cc_columns=cc_columns)

# COMMAND ----------

# Keep only antidepressant medication order records
med_depression = med_depression.where((F.col('antidepressant')))


# Columns on which to partition records for aggregation
partition_columns = ['pat_id', 'instance', 'lmp']
med_depression.createOrReplaceTempView("ad_org")


med_depression = spark.sql("""SELECT * FROM ad_org WHERE order_class != 'Historical Med' AND start_date IS NOT NULL AND end_date IS NOT NULL""")

def get_exposure_period(exposure_date):
  if -180 <= exposure_date < 0:
    return 0
  elif 0 <= exposure_date < 84:
    return 1 
  elif 84 <= exposure_date < 189:
    return 2
  elif 189 <= exposure_date <= 320:
    return 3
  else:
    return None
med_depression = med_depression.withColumn(
    'orderstart_lmp',
    F.datediff(F.col('start_date'), F.col('lmp'))).withColumn(
    'orderend_lmp',
    F.datediff(F.col('end_date'), F.col('lmp')))


med_depression = med_depression.withColumn('order_duration', F.datediff(F.col('end_date'), F.col('start_date'))+1) 

 
get_exposure_period_udf = F.udf(lambda exp: get_exposure_period(exp), StringType())

med_depression = med_depression.withColumn('orderstart_lmp_period', get_exposure_period_udf(F.col('orderstart_lmp'))).withColumn('orderend_lmp_period', get_exposure_period_udf(F.col('orderend_lmp'))).filter(F.col('start_date') >= F.date_sub(F.col('lmp'), 180)).filter(F.col('start_date') < F.col('ob_delivery_delivery_date'))




# COMMAND ----------

def get_exposure0_duration(orderstart_lmp, orderend_lmp):
  if (-180 <= orderstart_lmp < 0) and  (-180 <= orderend_lmp < 0):
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

med_depression = med_depression.withColumn('exposure0_duration', get_exposure0_duration_udf(F.col('orderstart_lmp'), F.col('orderend_lmp'))).withColumn('exposure1_duration',
                get_exposure1_duration_udf(F.col('orderstart_lmp'), F.col('orderend_lmp'))).withColumn('exposure2_duration',
                get_exposure2_duration_udf(F.col('orderstart_lmp'), F.col('orderend_lmp'))).withColumn('exposure3_duration',
                get_exposure3_duration_udf(F.col('orderstart_lmp'), F.col('orderend_lmp'), F.col('gestational_days')))

# COMMAND ----------

# MAGIC %run
# MAGIC "/Users/yeonmi.hwang@providence.org/Dissertation/Cloned/Clinical Concepts/Utilities/general"

# COMMAND ----------

write_data_frame_to_sandbox(med_depression , "yh_temp_med_sertraline_061923", sandbox_db='rdp_phi_sandbox', replace=True)

# COMMAND ----------


spark.sql("REFRESH TABLE rdp_phi_sandbox.yh_temp_med_sertraline_061923")
from pyspark.sql.functions import when
meds_org = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_temp_med_sertraline_061923")
meds_org = meds_org.withColumn('during_SSRI', when((F.col('antidepressant_N06AB'))&((F.col('exposure1_duration')>0)|(F.col('exposure2_duration')>0)|(F.col('exposure1_duration')>0)), 1).otherwise(0))
meds_org = meds_org.withColumn('during_sertraline', when((F.col('sertraline'))&((F.col('exposure1_duration')>0)|(F.col('exposure2_duration')>0)|(F.col('exposure1_duration')>0)), 1).otherwise(0))

# COMMAND ----------

partition_columns = ['pat_id', 'instance', 'lmp']
agg_cols = {'during_SSRI' : 'max', 'during_sertraline' : 'max'}
med_agg_org = aggregate_data(meds_org,
                          partition_columns = partition_columns, 
                          aggregation_columns = agg_cols)

# COMMAND ----------

depression_med = depression.join(med_agg_org, ['pat_id', 'instance', 'lmp'], 'left') 

# COMMAND ----------

depression_med = depression_med.fillna(0, ['during_SSRI_max', 'during_sertraline_max'])

# COMMAND ----------



# ----------------------------------------------------------------------------
# 3. run analysis 
from pyspark.sql.functions import col, create_map, lit
from itertools import chain

# COMMAND ----------

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
depression_med = convert_df(depression_med)

# COMMAND ----------

def add_geo_features(cohort_df, geo_df_name, join_cols = ['pat_id', 'instance']):
  geodf_list = ['ruca2010revised', 'countytypologycodes2015', 'farcodeszip2010', 'ruralurbancontinuumcodes2013', 'urbaninfluencecodes2013', 'svi2018_us', 'svi2018_us_county']
  master_patient = spark.sql("SELECT * FROM rdp_phi.dim_patient_master").select('pat_id','instance', 'PATIENT_STATE_CD', 'PATIENT_ADDR_CENSUS_BLOCKGROUP_DERIVED', 'ZIP')
  
  if geo_df_name not in geodf_list:
    print ('incorrect geo df name')
  else:
    geo_df = spark.sql("SELECT * from rdp_phi_sandbox.{0}".format(geo_df_name))
    if geo_df_name == 'ruca2010revised':
      geo_df = geo_df.withColumn('FIPS', F.col('State_County_Tract_FIPS_Code').cast(StringType())).drop('State_County_Tract_FIPS_Code')
      master_patient = master_patient.withColumn("FIPS", F.expr("CASE WHEN PATIENT_STATE_CD = 'CA' THEN substring(PATIENT_ADDR_CENSUS_BLOCKGROUP_DERIVED, 2, length(PATIENT_ADDR_CENSUS_BLOCKGROUP_DERIVED)-2) ELSE substring(PATIENT_ADDR_CENSUS_BLOCKGROUP_DERIVED, 0, length(PATIENT_ADDR_CENSUS_BLOCKGROUP_DERIVED)-1) END"))
      joined_df = master_patient.join(geo_df, 'FIPS', 'inner')
    elif geo_df_name == 'svi2018_us':
      master_patient = master_patient.withColumn("FIPS", F.expr("substring(PATIENT_ADDR_CENSUS_BLOCKGROUP_DERIVED, 0, length(PATIENT_ADDR_CENSUS_BLOCKGROUP_DERIVED)-1)"))
      joined_df = master_patient.join(geo_df, 'FIPS', 'inner') 
    elif ((geo_df_name == 'countytypologycodes2015')|(geo_df_name == 'urbaninfluencecodes2013')):
      geo_df = geo_df.withColumn('FIPS4', F.col('FIPStxt').cast(StringType())).drop('FIPStxt')
      master_patient = master_patient.withColumn("FIPS4", F.expr("CASE WHEN PATIENT_STATE_CD = 'CA' THEN substring(PATIENT_ADDR_CENSUS_BLOCKGROUP_DERIVED, 2, 4) ELSE substring(PATIENT_ADDR_CENSUS_BLOCKGROUP_DERIVED, 0, 5) END"))
      joined_df = master_patient.join(geo_df, 'FIPS4', 'inner')
    elif ((geo_df_name == 'svi2018_us_county')|(geo_df_name == 'ruralurbancontinuumcodes2013')):
      geo_df = geo_df.withColumn('FIPS5', F.col('FIPS').cast(StringType()))
      master_patient = master_patient.withColumn("FIPS5", F.expr("substring(PATIENT_ADDR_CENSUS_BLOCKGROUP_DERIVED, 0, 5)"))
      joined_df = master_patient.join(geo_df, 'FIPS5', 'inner')    
    elif geo_df_name == 'farcodeszip2010':
      geo_df = geo_df.withColumn('ZIP5', F.col('ZIP').cast(StringType())).drop('ZIP')
      master_patient = master_patient.withColumn("ZIP5", F.expr("substring(ZIP, 0, 5)")).drop('ZIP')
      joined_df = master_patient.join(geo_df, 'ZIP5', 'inner')
    return_df = cohort_df.join(joined_df, join_cols, 'left')
  return return_df 

def categorize_ruca(code):
  if code is None:
    return None
  elif code < 4:
    return 'Metropolitan'
  elif code < 7:
    return 'Micropolitan'
  elif code < 10:
    return 'SmallTown'
  elif code < 99:
    return 'Rural'
  elif code == 99:
    return 'NotCoded'
  else:
    return None 
categorize_ruca_udf = F.udf(lambda code: categorize_ruca(code), StringType())

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

def intersection(lst1, lst2): 
    return list(set(lst1) & set(lst2)) 

# COMMAND ----------

basic_columns = ['delivery_year','ob_hx_infant_sex', 'ethnic_group', 'age_at_start_dt', 'race_group', 'Parity', 'Gravidity', 'insurance', 'pregravid_bmi', 'Preterm_history']
basic_binary_columns = ['smoker', 'illegal_drug_user', 'alcohol_user']
columns_including = ['PTB_category', 'SGA_category', 'LBW_category', 'gestational_days']
med_list = ['during_SSRI_max', 'during_sertraline_max']

# COMMAND ----------

select_columns = list(set(basic_columns + basic_binary_columns + columns_including + med_list))

# COMMAND ----------

ps_cohort, full_covariate = format_spark_dataframe_for_psm(depression_med, select_columns, med_list, columns_including)

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

full_covariate

# COMMAND ----------

ps_cohort_pd = ps_cohort.toPandas()

# COMMAND ----------

# ps_cohort_pd.during_sertraline_max.value_counts()

# COMMAND ----------

# ps_cohort_pd.during_SSRI_max.value_counts()

# COMMAND ----------

ps_df_sertraline = get_propensity_score('during_sertraline_max', full_covariate, ps_cohort_pd)
ps_df_SSRI = get_propensity_score('during_SSRI_max', full_covariate, ps_cohort_pd)

# COMMAND ----------

ps_cohort_pd.columns

# COMMAND ----------

def propensity_score_adjustment(df, x , outcome):
  import statsmodels.formula.api as smf
  import numpy as np
  df['propensity_score'] = pd.to_numeric(df['propensity_score'])
  df[outcome] = pd.to_numeric(df[outcome])
  df[x] = pd.to_numeric(df[x])
  model= smf.logit(formula="{0} ~ {1} + propensity_score".format(outcome, x), data= df).fit()
  model_odds = pd.DataFrame(np.exp(model.params), columns= ['OR'])
  model_odds['z-value']= model.pvalues
  model_odds[['2.5%', '97.5%']] = np.exp(model.conf_int())
  model_odds
  return model_odds 

# COMMAND ----------

# SSRI_model_odds = propensity_score_adjustment(ps_df_SSRI, 'during_SSRI_max', 'PTB_category')

# COMMAND ----------

# SSRI_model_odds

# COMMAND ----------

# sertraline_model_odds = propensity_score_adjustment(ps_df_sertraline, 'during_sertraline_max', 'PTB_category')
# sertraline_model_odds