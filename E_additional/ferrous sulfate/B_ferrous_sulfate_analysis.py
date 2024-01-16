# Author: Yeon Mi Hwang  
# Date: 01/15/2024
# Disclaimer : This code was originally written in databricks workspace. 
# Objective of B_ferrous_sulfate_analysis.py : To run analysis for ferrous sulfate
# Workflow of B_ferrous_sulfate_analysis.py
## 1. load necessary packages and functions 
## 2. load relevant dataframe 
## 3. run analysis 



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

total = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_meds_diagnoses_op_2022").select(*select_columns)

# COMMAND ----------

prenatal_anemia = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_prenatal_anemia").withColumnRenamed('iron_x_anemia', 'prenatal_iron_x_anemia')
prepregnancy_anemia = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_prepregnancy_anemia_062423").withColumnRenamed('iron_x_anemia', 'prepregnancy_iron_x_anemia')

# COMMAND ----------

total_joined = total.join(prenatal_anemia, ['patient_id', 'lmp', 'ob_delivery_delivery_date'], 'left').join(prepregnancy_anemia, ['patient_id', 'lmp', 'ob_delivery_delivery_date'], 'left').fillna(0, ['prenatal_anemia', 'prepregnancy_anemia', 'prenatal_iron_x_anemia', 'prepregnancy_iron_x_anemia'])

# COMMAND ----------

anemia = total_joined.filter(F.col('prepregnancy_iron_x_anemia')==1)

# COMMAND ----------

filter_string = \
"""
date_sub(lmp, 180) <= start_date AND start_date <= ob_delivery_delivery_date
"""

# COMMAND ----------

med_anemia = get_medication_orders(
  cohort_df= anemia,
  include_cohort_columns=select_columns,
  filter_string = filter_string)

# COMMAND ----------

ferrous_sulfate = med_anemia.filter(F.lower(F.col('order_description')).contains('ferrous sulfate'))
ferrous_sulfate_op = ferrous_sulfate.filter(F.col('order_mode')=='Outpatient')

# COMMAND ----------

partition_columns = ['pat_id', 'instance', 'lmp']
ferrous_sulfate.createOrReplaceTempView("ferrous_sulfate")
ferrous_sulfate_op.createOrReplaceTempView("ferrous_sulfate_op")


med_ferrous_sulfate = spark.sql("""SELECT * FROM ferrous_sulfate WHERE order_class != 'Historical Med' AND start_date IS NOT NULL AND end_date IS NOT NULL""")
med_ferrous_sulfate_op = spark.sql("""SELECT * FROM ferrous_sulfate_op WHERE order_class != 'Historical Med' AND start_date IS NOT NULL AND end_date IS NOT NULL""")

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
med_ferrous_sulfate = med_ferrous_sulfate.withColumn(
    'orderstart_lmp',
    F.datediff(F.col('start_date'), F.col('lmp'))).withColumn(
    'orderend_lmp',
    F.datediff(F.col('end_date'), F.col('lmp')))

med_ferrous_sulfate_op = med_ferrous_sulfate_op.withColumn(
    'orderstart_lmp',
    F.datediff(F.col('start_date'), F.col('lmp'))).withColumn(
    'orderend_lmp',
    F.datediff(F.col('end_date'), F.col('lmp')))


med_ferrous_sulfate = med_ferrous_sulfate.withColumn('order_duration', F.datediff(F.col('end_date'), F.col('start_date'))+1) 
med_ferrous_sulfate_op = med_ferrous_sulfate_op.withColumn('order_duration', F.datediff(F.col('end_date'), F.col('start_date'))+1) 
 
get_exposure_period_udf = F.udf(lambda exp: get_exposure_period(exp), StringType())

med_ferrous_sulfate = med_ferrous_sulfate.withColumn('orderstart_lmp_period', get_exposure_period_udf(F.col('orderstart_lmp'))).withColumn('orderend_lmp_period', get_exposure_period_udf(F.col('orderend_lmp'))).filter(F.col('start_date') >= F.date_sub(F.col('lmp'), 180)).filter(F.col('start_date') < F.col('ob_delivery_delivery_date'))
med_ferrous_sulfate_op = med_ferrous_sulfate_op.withColumn('orderstart_lmp_period', get_exposure_period_udf(F.col('orderstart_lmp'))).withColumn('orderend_lmp_period', get_exposure_period_udf(F.col('orderend_lmp'))).filter(F.col('start_date') >= F.date_sub(F.col('lmp'), 180)).filter(F.col('start_date') < F.col('ob_delivery_delivery_date'))

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

med_ferrous_sulfate = med_ferrous_sulfate.withColumn('exposure0_duration', get_exposure0_duration_udf(F.col('orderstart_lmp'), F.col('orderend_lmp'))).withColumn('exposure1_duration',
                get_exposure1_duration_udf(F.col('orderstart_lmp'), F.col('orderend_lmp'))).withColumn('exposure2_duration',
                get_exposure2_duration_udf(F.col('orderstart_lmp'), F.col('orderend_lmp'))).withColumn('exposure3_duration',
                get_exposure3_duration_udf(F.col('orderstart_lmp'), F.col('orderend_lmp'), F.col('gestational_days')))



med_ferrous_sulfate_op = med_ferrous_sulfate_op.withColumn('exposure0_duration', get_exposure0_duration_udf(F.col('orderstart_lmp'), F.col('orderend_lmp'))).withColumn('exposure1_duration',
                get_exposure1_duration_udf(F.col('orderstart_lmp'), F.col('orderend_lmp'))).withColumn('exposure2_duration',
                get_exposure2_duration_udf(F.col('orderstart_lmp'), F.col('orderend_lmp'))).withColumn('exposure3_duration',
                get_exposure3_duration_udf(F.col('orderstart_lmp'), F.col('orderend_lmp'), F.col('gestational_days')))

# COMMAND ----------

# MAGIC %run
# MAGIC "/Users/yeonmi.hwang@providence.org/Dissertation/Cloned/Clinical Concepts/Utilities/general"

# COMMAND ----------

write_data_frame_to_sandbox(med_ferrous_sulfate , "yh_temp_med_ferrous_sulfate_061923", sandbox_db='rdp_phi_sandbox', replace=True)

# COMMAND ----------

write_data_frame_to_sandbox(med_ferrous_sulfate_op , "yh_temp_med_ferrous_sulfate_op_061923", sandbox_db='rdp_phi_sandbox', replace=True)

# COMMAND ----------


spark.sql("REFRESH TABLE rdp_phi_sandbox.yh_temp_med_ferrous_sulfate_061923")
from pyspark.sql.functions import when
ferrous_sulfate = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_temp_med_ferrous_sulfate_061923")
ferrous_sulfate = ferrous_sulfate.withColumn('during_ferrous_sulfate', when(((F.col('exposure1_duration')>0)|(F.col('exposure2_duration')>0)|(F.col('exposure1_duration')>0)), 1).otherwise(0))

# COMMAND ----------


spark.sql("REFRESH TABLE rdp_phi_sandbox.yh_temp_med_ferrous_sulfate_op_061923")
from pyspark.sql.functions import when
ferrous_sulfate_op = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_temp_med_ferrous_sulfate_op_061923")
ferrous_sulfate_op = ferrous_sulfate_op.withColumn('during_ferrous_sulfate_op', when(((F.col('exposure1_duration')>0)|(F.col('exposure2_duration')>0)|(F.col('exposure1_duration')>0)), 1).otherwise(0))

# COMMAND ----------

partition_columns = ['pat_id', 'instance', 'lmp']
agg_cols = {'during_ferrous_sulfate' : 'max'}
ferrous_sulfate_agg = aggregate_data(ferrous_sulfate,
                          partition_columns = partition_columns, 
                          aggregation_columns = agg_cols)

# COMMAND ----------

partition_columns = ['pat_id', 'instance', 'lmp']
agg_cols = {'during_ferrous_sulfate_op' : 'max'}
ferrous_sulfate_op_agg = aggregate_data(ferrous_sulfate_op,
                          partition_columns = partition_columns, 
                          aggregation_columns = agg_cols)

# COMMAND ----------

anemia_med = anemia.join(ferrous_sulfate_agg, ['pat_id', 'instance', 'lmp'], 'left').join(ferrous_sulfate_op_agg, ['pat_id', 'instance', 'lmp'], 'left') 

# COMMAND ----------

anemia_med = anemia_med.fillna(0, ['during_ferrous_sulfate_max', 'during_ferrous_sulfate_op_max'])

# ----------------------------------------------------------------------------
# 2. run analysis 


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
anemia_med = convert_df(anemia_med)

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

basic_columns = ['delivery_year','ob_hx_infant_sex', 'ethnic_group', 'age_at_start_dt', 'race_group', 'Parity', 'Gravidity', 'insurance', 'pregravid_bmi', 'Preterm_history']
basic_binary_columns = ['smoker', 'illegal_drug_user', 'alcohol_user']
columns_including = ['PTB_category', 'SGA_category', 'LBW_category', 'gestational_days', 'prenatal_anemia']
med_list = ['during_ferrous_sulfate_max', 'during_ferrous_sulfate_op_max']

# COMMAND ----------

select_columns = list(set(basic_columns + basic_binary_columns + columns_including + med_list))

# COMMAND ----------

anemia_sa = anemia_med.filter(F.col('prenatal_iron_x_anemia')==1)

# COMMAND ----------

# anemia_sa.count()

# COMMAND ----------

# anemia_med.count()

# COMMAND ----------

ps_cohort, full_covariate = format_spark_dataframe_for_psm(anemia_med, select_columns, med_list, columns_including)

# COMMAND ----------

ps_cohort_sa, full_covariate = format_spark_dataframe_for_psm(anemia_sa, select_columns, med_list, columns_including)

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

ps_cohort_pd = ps_cohort.toPandas()
ps_cohort_sa_pd = ps_cohort_sa.toPandas()

# COMMAND ----------

ps_df = get_propensity_score('during_ferrous_sulfate_max', full_covariate, ps_cohort_pd)
ps_df_sa = get_propensity_score('during_ferrous_sulfate_max', full_covariate, ps_cohort_sa_pd)
ps_op_df = get_propensity_score('during_ferrous_sulfate_op_max', full_covariate, ps_cohort_pd)
ps_op_df_sa = get_propensity_score('during_ferrous_sulfate_op_max', full_covariate, ps_cohort_sa_pd)

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

# ferrous_sulfate_model_odds = propensity_score_adjustment(ps_df, 'during_ferrous_sulfate_max', 'PTB_category')
# ferrous_sulfate_model_odds

# COMMAND ----------

# ferrous_sulfate_op_model_odds = propensity_score_adjustment(ps_op_df, 'during_ferrous_sulfate_op_max', 'PTB_category')
# ferrous_sulfate_op_model_odds

# COMMAND ----------

# ferrous_sulfate_model_odds_sa = propensity_score_adjustment(ps_df_sa, 'during_ferrous_sulfate_max', 'PTB_category')
# ferrous_sulfate_model_odds_sa

# COMMAND ----------

# ferrous_sulfate_op_model_odds_sa = propensity_score_adjustment(ps_op_df_sa, 'during_ferrous_sulfate_op_max', 'PTB_category')
# ferrous_sulfate_op_model_odds_sa