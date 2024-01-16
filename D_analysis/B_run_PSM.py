# Author: Yeon Mi Hwang  
# Date: 01/15/2024
# Disclaimer : This code was originally written in databricks workspace. 
# Objective of B_run_PSM.py : run propensity score matching 
# Workflow of B_run_PSM.py
## 1. load necessary packages and functions 
## 2. load dataframes 
## 3. load prepare dataframe for propensity score matching 
## 4. run propensity score matching 





# ----------------------------------------------------------------------------
# 1. load necessary packages and functions 


# Databricks notebook source
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import *
import CEDA_Tools_v1_1.load_ceda_etl_tools

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)



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

# ----------------------------------------------------------------------------
# 2. load dataframe


mom_diagnoses_op_1 = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_meds_diagnoses_op_1_2022")
mom_diagnoses_op_total = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_meds_diagnoses_op_2022")
op_total = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_meds_op_total_new")

moms = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_2022")
prepregnancy_diagnoses = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_2022_prepregnancy_diagnosis")
prenatal_diagnoses = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_2022_prenatal_diagnosis")
mom_diagnoses1 = moms.join(prepregnancy_diagnoses, intersection(moms.columns, prepregnancy_diagnoses.columns), 'left')
mom_diagnoses2 = mom_diagnoses1.join(prenatal_diagnoses, intersection(mom_diagnoses1.columns, prenatal_diagnoses.columns), 'left')

# ----------------------------------------------------------------------------
# 3. prepare dataframe for propensity score matching 

mom_diagnoses_op_1 = convert_df(mom_diagnoses_op_1)
mom_diagnoses_op_total = convert_df(mom_diagnoses_op_total)




import sys
sys.setrecursionlimit(100000)


def iterative_impute_missing_data(df):
  from sklearn.experimental import enable_iterative_imputer
  from sklearn.impute import IterativeImputer
  imputer = IterativeImputer(max_iter = 10, random_state=42)
  df_return = ps.DataFrame(imputer.fit_transform(df), columns=df.columns)
  return(df_return)


def intersection(lst1, lst2): 
    return list(set(lst1) & set(lst2)) 



def consolidate_race_responses(l):
  l_new = []
  for i in l:
    if i == 'White':
      l_new.append('White or Caucasian')
    elif i == 'Patient Refused' or i == 'Unable to Determine' or i == 'Declined' or i == 'Unknown':
      continue
    else:
      l_new.append(i)
  l_new = list(set(l_new))
  return(l_new)


def handle_multiracial_exceptions(l):
  l_new = consolidate_race_responses(l)
  if l_new is None:
    return('Unknown')
  if len(l_new) == 1:
    return(l_new[0])
  if 'Other' in l_new:
    l_new.remove('Other')
    if l_new is None:
      return('Other')
    if len(l_new) == 1:
      return(l_new[0])
  return('Multiracial')


def format_race(i):
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


def format_parity(i):
  if i is None:
    return 0
  if math.isnan(i):
    return 0
  i = int(i)
  if i == 0 or i == 1:
    return 0
  if i > 1 and i < 5:
    return 1
  if i >= 5:
    return 2
  return 0


def format_gravidity(gravidity):
  if gravidity is None:
    return 0
  if math.isnan(gravidity):
    return 0
  gravidity = int(gravidity)
  if gravidity == 0 or gravidity == 1:
    return 0
  elif gravidity > 1 and gravidity < 6:
    return 1
  elif gravidity >= 6:
    return 2
  return 0
    
  
def format_preterm_history(preterm_history, gestational_days):
  import math
  if preterm_history is None:
    return 0
  if math.isnan(preterm_history):
    return 0
  else:
    preterm_history = int(preterm_history)
    if preterm_history == 0 or (preterm_history == 1 and gestational_days < 259):
      return 0
    else:
      return 1
  return 0


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
  if i in list_c_section:
    return(1)
  return(-1)


def encode_bmi(bmi):
  if bmi is None or math.isnan(bmi):
    return -1
  bmi = int(bmi)
  if bmi >= 15 and bmi < 18.5:
    return 0
  if bmi < 25:
    return 1
  if bmi < 30:
    return 2
  if bmi < 35:
    return 3
  if bmi < 40:
    return 4
  return -1

def encode_ruca(ruca):
  if ruca is None:
    return -1
  if ruca == 'Rural':
    return 0
  if ruca == 'SmallTown':
    return 1
  if ruca == 'Micropolitan':
    return 2
  if ruca == 'Metropolitan':
    return 3
  return -1

def encode_age(age):
  if age < 25:
    return 0
  if age < 30:
    return 1
  if age < 35:
    return 2
  if age < 40:
    return 3
  if age < 45:
    return 4
  return -1


def select_psm_columns(df, columns):
  return_df = df.select(*columns)
  return return_df


def handle_missing_bmi(df):
  print('# Percent of patients with pregravid BMI:', str(round(100*(len(df) - df['pregravid_bmi'].isna().sum())/len(df), 1)), '%')
  print('Imputing median pregravid BMI of', str(round(df['pregravid_bmi'].median(), 2)), '...')
  df['pregravid_bmi'].fillna(df['pregravid_bmi'].median(), inplace = True)
  print('\n')
  return df

def handle_missing_svi(df, col):
  print('# Percent of patients with svi:', str(round(100*(len(df) - df[col].isna().sum())/len(df), 1)), '%')
  print('Imputing median svi of', str(round(df[col].median(), 2)), '...')
  df[col].fillna(df[col].median(), inplace = True)
  print('\n')
  return df

from sklearn.preprocessing import MinMaxScaler

def format_dataframe_for_psm(pd_df, select_columns, med_columns, result_columns):
  dict_white = {'White or Caucasian': 1, 'Unknown': 0, 'Asian': 0, 'Multiracial': 0, 'Other': 0, 'Black or African American': 0, 'Native Hawaiian or Other Pacific Islander': 0, 'American Indian or Alaska Native': 0}
  dict_asian = {'White or Caucasian': 0, 'Unknown': 0, 'Asian': 1, 'Multiracial': 0, 'Other': 0, 'Black or African American': 0, 'Native Hawaiian or Other Pacific Islander': 0, 'American Indian or Alaska Native': 0}
  dict_other = {'White or Caucasian': 0, 'Unknown': 0, 'Asian': 0, 'Multiracial': 0, 'Other': 1, 'Black or African American': 0, 'Native Hawaiian or Other Pacific Islander': 0, 'American Indian or Alaska Native': 0}
  dict_black = {'White or Caucasian': 0, 'Unknown': 0, 'Asian': 0, 'Multiracial': 0, 'Other': 0, 'Black or African American': 1, 'Native Hawaiian or Other Pacific Islander': 0, 'American Indian or Alaska Native': 0}
  dict_ethnic_groups = {None:-1, 'Unknown_NotReported': -1, 'Hispanic_Latino': 1, 'Not_Hispanic_Latino': 0}
  dict_fetal_sex = {None: -1, 'Male': 1, 'Female': 0, 'Other': -1, 'Unknown': -1}
  dict_commercial_insurance = {'Medicaid': 0, 'Medicare': 0, 'Uninsured-Self-Pay': 0, None: 0, 'Other': 0, 'Commercial': 1}
  dict_governmental_insurance = {'Medicaid': 1, 'Medicare': 0, 'Uninsured-Self-Pay': 0, None: 0, 'Other': 0, 'Commercial': 0}
  intact_columns = med_columns + result_columns
  select_columns = list(set(select_columns) - set(intact_columns))
  intact_pd = pd_df[intact_columns]
  selected_pd = pd_df[select_columns]
  for index, row in selected_pd.iterrows():
    selected_pd.at[index, 'race_group'] = format_race(row['race_group'])
    selected_pd.at[index, 'Preterm_history'] = format_preterm_history((row['Preterm_history']), row['gestational_days'])
  for index, row in selected_pd.iterrows():
    selected_pd.at[index, 'race_white'] = dict_white[row['race_group']]
    selected_pd.at[index, 'race_asian'] = dict_asian[row['race_group']]
    selected_pd.at[index, 'race_black'] = dict_black[row['race_group']]
    selected_pd.at[index, 'race_other'] = dict_other[row['race_group']]
    selected_pd.at[index, 'ethnic_group'] = dict_ethnic_groups[row['ethnic_group']]
    selected_pd.at[index, 'ob_hx_infant_sex'] = dict_fetal_sex[row['ob_hx_infant_sex']]
    selected_pd.at[index, 'commercial_insurance'] = dict_commercial_insurance[row['insurance']]
    selected_pd.at[index, 'Parity'] = format_parity(row['Parity'])
    selected_pd.at[index, 'Gravidity'] = format_gravidity(row['Gravidity'])
    selected_pd.at[index, 'pregravid_bmi'] = encode_bmi(row['pregravid_bmi'])
    selected_pd.at[index, 'age_at_start_dt'] = encode_age(row['age_at_start_dt'])
  selected_pd = selected_pd.drop(columns=['gestational_days', 'insurance', 'race_group'])
  selected_pd = selected_pd.fillna(-1)
  print('Columns used for matching:')
  for col in selected_pd.columns:
    print(col)
  print('\n')
  print('\n')
  final_pd = pd.concat([intact_pd, selected_pd], axis=1)
  return(final_pd, selected_pd.columns)



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

# COMMAND ----------

basic_columns = ['delivery_year','ob_hx_infant_sex', 'ethnic_group', 'age_at_start_dt', 'race_group', 'Parity', 'Gravidity', 'insurance', 'pregravid_bmi', 'Preterm_history', 'gestational_days']
basic_binary_columns = ['smoker', 'illegal_drug_user', 'alcohol_user']
comorbidity_binary_columns = ['prepregnancy_chronic_kidney_disease','prepregnancy_diabetes','prepregnancy_leukemia','prepregnancy_pneumonia','prepregnancy_sepsis','prepregnancy_cardiovascular_diseases','prepregnancy_anemia','prepregnancy_sickle_cell_diseases','prepregnancy_cystic_fibrosis', 'prepregnancy_asthma']
prenatal_diagnoses_binary_columns = [ 'prenatal_history_csec','prenatal_diabetes','prenatal_preterm_labor', 'prenatal_anemia', 'prenatal_bacterial_infection', 'prenatal_rhd_negative','prenatal_breech_presentation','prenatal_abnormal_fetal_movement','prenatal_hypertensive_disorder', 'prenatal_poor_fetal_growth','prenatal_placenta_previa','prenatal_anxiety_depression','prenatal_excessive_fetal_growth','prenatal_prom',  'prenatal_hypertensive_disorder']
prepregnancy_diagnoses_binary_columns = ['prepregnancy_diabetes','prepregnancy_anemia', 'prepregnancy_bacterial_infection',  'prepregnancy_vitamin_d_deficiency', 'prepregnancy_gerd','prepregnancy_irregular_period_codes', 'prepregnancy_fatigue', 'prepregnancy_iud','prepregnancy_asthma','prepregnancy_anxiety_depression','prepregnancy_hypertensive_disorder', 'prepregnancy_hypothyroidism',  'prepregnancy_insomina']
med_list = list(set(op_total.columns)-set(intersection(op_total.columns, mom_diagnoses2.columns)))
columns_including = ['PTB_category', 'SGA_category', 'LBW_category']

# COMMAND ----------

comorbidities = comorbidity_binary_columns +prenatal_diagnoses_binary_columns + prepregnancy_diagnoses_binary_columns
comorbidities.sort()
comorbidities

# COMMAND ----------

full_diagnoses_select_columns = list(set(basic_columns + basic_binary_columns + comorbidity_binary_columns +prenatal_diagnoses_binary_columns + prepregnancy_diagnoses_binary_columns + med_list + columns_including))

# COMMAND ----------

psm_op_total_cohort, full_covariate = format_spark_dataframe_for_psm(mom_diagnoses_op_total , full_diagnoses_select_columns, med_list, columns_including)

# COMMAND ----------

psm_op_total_cohort.columns

# COMMAND ----------

# write_data_frame_to_sandbox(psm_op_total_cohort, "yh_maternity_op_diagnoses_psm", sandbox_db='rdp_phi_sandbox', replace=True)

# COMMAND ----------

mom_diagnoses_op_total_pd = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_op_diagnoses_psm").drop('admission_datetime_filled').toPandas()

# ----------------------------------------------------------------------------
# 4. run propensity score matching  

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

def get_pval (rr, CI_L, CI_U):
  se = (CI_U-CI_L)/(2*1.96)
  z = rr / se
  p = np.exp(-0.717*z - 0.415*z**2)
  return p 


def get_pval_signal (p):
  if p>0.1:
    return 'ns'
  elif p>0.05:
    return '+'
  elif p>0.01:
    return '*'
  elif p>0.001:
    return '**'
  elif p>0.0001:
    return '***'
  else:
    return '****'

def get_relative_risk(outcome, matched_pd, med):
  from scipy.stats.contingency import relative_risk
  from scipy.stats import fisher_exact
  crosstab = pd.crosstab(matched_pd[med],matched_pd[outcome])
  rr = relative_risk(crosstab.iloc[1,1], crosstab.iloc[1,0]+crosstab.iloc[1,1], crosstab.iloc[0,1], crosstab.iloc[0,0]+crosstab.iloc[0,1])
  unexposed_prevalence = crosstab.iloc[0,1]/(crosstab.iloc[0,0]+crosstab.iloc[0,1])
  exposed_prevalence = crosstab.iloc[1,1]/(crosstab.iloc[1,0]+crosstab.iloc[1,1])
  CI_L = rr.confidence_interval(confidence_level=0.95)[0]
  CI_U = rr.confidence_interval(confidence_level=0.95)[1]
  p = fisher_exact(crosstab)[1]
  return rr.relative_risk, unexposed_prevalence, exposed_prevalence, CI_L, CI_U, p  
def calculate_effectsize(matched_df, group, variables):
  efts = []
  for v in variables:
    x = matched_df[matched_df[group]==1][v]
    y = matched_df[matched_df[group]==0][v]
    eft = pg.compute_effsize(x, y, eftype='cohen')
    import math
    if math.isnan (eft):
      eft = 0
    efts.append(eft)
  return (np.nanmean(np.array(efts)))

# COMMAND ----------

def run_PSM(cohort_table, med_list, covariate):
  PSM_dict = {}
  cohort_table = cohort_table[med_list + covariate + columns_including]
  for med in med_list: 
    print (med)
    if cohort_table[med].sum()>600:
      ps = get_propensity_score(med,list(set(covariate)-set(['index'])), cohort_table)
      indices, matched = propensity_score_matching(ps, med) 
      PSM_dict[med] = matched
  return PSM_dict

# COMMAND ----------

def get_PSM_outcome(PSM_dict, covariate):
  import pingouin as pg
  from scipy.stats.contingency import relative_risk
  result = {}
  variable = []
  ES = []
  PTB_RR = []
  PTB_p = []
  PTB_p_signal = []
  PTB_RR_CIL = []
  PTB_RR_CIU = []
  exposed_PTB_prevalence = []
  unexposed_PTB_prevalence = []
  SGA_RR = []
  SGA_p = []
  SGA_p_signal = []
  SGA_RR_CIL = []
  SGA_RR_CIU = []
  exposed_SGA_prevalence = []
  unexposed_SGA_prevalence = []
  LBW_RR = []
  LBW_p = []
  LBW_p_signal = []
  LBW_RR_CIL = []
  LBW_RR_CIU = []
  exposed_LBW_prevalence = []
  unexposed_LBW_prevalence = []
  for med in PSM_dict.keys():
    print (med)
    matched = PSM_dict[med]
    matched = matched.apply(pd.to_numeric, errors='coerce')
    es = calculate_effectsize(matched, med, covariate)             
    ptb_rr, ptb_unexposed, ptb_exposed, ptb_CIL, ptb_CIU, ptb_p = get_relative_risk('PTB_category', matched, med)
    sga_rr, sga_unexposed, sga_exposed, sga_CIL, sga_CIU, sga_p = get_relative_risk('SGA_category', matched, med)
    lbw_rr, lbw_unexposed, lbw_exposed, lbw_CIL, lbw_CIU, lbw_p = get_relative_risk('LBW_category', matched, med)
    variable.append(med)
    ES.append(es)
    PTB_RR.append(np.round(ptb_rr,2))
    SGA_RR.append(np.round(sga_rr,2))
    LBW_RR.append(np.round(lbw_rr,2))
    PTB_RR_CIL.append(np.round(ptb_CIL,2))
    PTB_RR_CIU.append(np.round(ptb_CIU,2))
    SGA_RR_CIL.append(np.round(sga_CIL,2))
    SGA_RR_CIU.append(np.round(sga_CIU,2))
    LBW_RR_CIL.append(np.round(lbw_CIL,2))  
    LBW_RR_CIU.append(np.round(lbw_CIU,2))
    PTB_p.append(ptb_p)
    SGA_p.append(sga_p)
    LBW_p.append(lbw_p)
    PTB_p_signal.append(get_pval_signal(ptb_p))
    SGA_p_signal.append(get_pval_signal(sga_p))
    LBW_p_signal.append(get_pval_signal(lbw_p))
    unexposed_PTB_prevalence.append(ptb_unexposed)
    exposed_PTB_prevalence.append(ptb_exposed)
    unexposed_SGA_prevalence.append(sga_unexposed)
    exposed_SGA_prevalence.append(sga_exposed)
    unexposed_LBW_prevalence.append(lbw_unexposed)
    exposed_LBW_prevalence.append(lbw_exposed)
  result['medication'] = variable
  result['es'] = ES
  result['PTB_RR'] = PTB_RR
  result['PTB_p'] = PTB_p
  result['PTB_p_signal'] = PTB_p_signal
  result['PTB_RR_CI_L'] = PTB_RR_CIL
  result['PTB_RR_CI_U'] = PTB_RR_CIU
  result['SGA_RR'] = SGA_RR
  result['SGA_p'] = SGA_p
  result['SGA_p_signal'] = SGA_p_signal
  result['SGA_RR_CI_L'] = SGA_RR_CIL
  result['SGA_RR_CI_U'] = SGA_RR_CIU
  result['LBW_RR'] = LBW_RR
  result['LBW_p'] = LBW_p
  result['LBW_p_signal'] = LBW_p_signal
  result['LBW_RR_CI_L'] = LBW_RR_CIL
  result['LBW_RR_CI_U'] = LBW_RR_CIU
  result['PTB_unexposed'] = unexposed_PTB_prevalence
  result['PTB_exposed'] = exposed_PTB_prevalence
  result['SGA_unexposed'] = unexposed_SGA_prevalence
  result['SGA_exposed'] = exposed_SGA_prevalence
  result['LBW_unexposed'] = unexposed_LBW_prevalence
  result['LBW_exposed'] = exposed_LBW_prevalence
  result_df = pd.DataFrame(result) 
  return (result_df)

# COMMAND ----------

mom_diagnoses_op_total_pd

# COMMAND ----------

def get_relative_risk(outcome, matched_pd, med):
  from scipy.stats.contingency import relative_risk
  from scipy.stats import fisher_exact
  crosstab = pd.crosstab(matched_pd[med],matched_pd[outcome])
  rr = relative_risk(crosstab.iloc[1,1], crosstab.iloc[1,0]+crosstab.iloc[1,1], crosstab.iloc[0,1], crosstab.iloc[0,0]+crosstab.iloc[0,1])
  unexposed_prevalence = crosstab.iloc[0,1]/(crosstab.iloc[0,0]+crosstab.iloc[0,1])
  exposed_prevalence = crosstab.iloc[1,1]/(crosstab.iloc[1,0]+crosstab.iloc[1,1])
  CI_L = rr.confidence_interval(confidence_level=0.95)[0]
  CI_U = rr.confidence_interval(confidence_level=0.95)[1]
  p = fisher_exact(crosstab)[1]
  return rr.relative_risk, unexposed_prevalence, exposed_prevalence, CI_L, CI_U, p  

# COMMAND ----------

def get_unadjusted_outcome(df, med_list): 
  import pingouin as pg
  from scipy.stats.contingency import relative_risk
  result = {}
  variable = []
  PTB_RR = []
  PTB_p = []
  PTB_p_signal = []
  PTB_RR_CIL = []
  PTB_RR_CIU = []
  exposed_PTB_prevalence = []
  unexposed_PTB_prevalence = []
  SGA_RR = []
  SGA_p = []
  SGA_p_signal = []
  SGA_RR_CIL = []
  SGA_RR_CIU = []
  exposed_SGA_prevalence = []
  unexposed_SGA_prevalence = []
  LBW_RR = []
  LBW_p = []
  LBW_p_signal = []
  LBW_RR_CIL = []
  LBW_RR_CIU = []
  exposed_LBW_prevalence = []
  unexposed_LBW_prevalence = []
  for med in med_list:
    if df[med].sum()>600:
      print (med)
      ptb_rr, ptb_unexposed, ptb_exposed, ptb_CIL, ptb_CIU, ptb_p = get_relative_risk('PTB_category', df, med)
      sga_rr, sga_unexposed, sga_exposed, sga_CIL, sga_CIU, sga_p = get_relative_risk('SGA_category', df, med)
      lbw_rr, lbw_unexposed, lbw_exposed, lbw_CIL, lbw_CIU, lbw_p = get_relative_risk('LBW_category', df, med)
      variable.append(med)
      PTB_RR.append(np.round(ptb_rr,2))
      SGA_RR.append(np.round(sga_rr,2))
      LBW_RR.append(np.round(lbw_rr,2))
      PTB_RR_CIL.append(np.round(ptb_CIL,2))
      PTB_RR_CIU.append(np.round(ptb_CIU,2))
      SGA_RR_CIL.append(np.round(sga_CIL,2))
      SGA_RR_CIU.append(np.round(sga_CIU,2))
      LBW_RR_CIL.append(np.round(lbw_CIL,2))  
      LBW_RR_CIU.append(np.round(lbw_CIU,2))
      PTB_p.append(ptb_p)
      SGA_p.append(sga_p)
      LBW_p.append(lbw_p)
      PTB_p_signal.append(get_pval_signal(ptb_p))
      SGA_p_signal.append(get_pval_signal(sga_p))
      LBW_p_signal.append(get_pval_signal(lbw_p))
      unexposed_PTB_prevalence.append(ptb_unexposed)
      exposed_PTB_prevalence.append(ptb_exposed)
      unexposed_SGA_prevalence.append(sga_unexposed)
      exposed_SGA_prevalence.append(sga_exposed)
      unexposed_LBW_prevalence.append(lbw_unexposed)
      exposed_LBW_prevalence.append(lbw_exposed)
  result['medication'] = variable
  result['PTB_RR'] = PTB_RR
  result['PTB_p'] = PTB_p
  result['PTB_p_signal'] = PTB_p_signal
  result['PTB_RR_CI_L'] = PTB_RR_CIL
  result['PTB_RR_CI_U'] = PTB_RR_CIU
  result['SGA_RR'] = SGA_RR
  result['SGA_p'] = SGA_p
  result['SGA_p_signal'] = SGA_p_signal
  result['SGA_RR_CI_L'] = SGA_RR_CIL
  result['SGA_RR_CI_U'] = SGA_RR_CIU
  result['LBW_RR'] = LBW_RR
  result['LBW_p'] = LBW_p
  result['LBW_p_signal'] = LBW_p_signal
  result['LBW_RR_CI_L'] = LBW_RR_CIL
  result['LBW_RR_CI_U'] = LBW_RR_CIU
  result['PTB_unexposed'] = unexposed_PTB_prevalence
  result['PTB_exposed'] = exposed_PTB_prevalence
  result['SGA_unexposed'] = unexposed_SGA_prevalence
  result['SGA_exposed'] = exposed_SGA_prevalence
  result['LBW_unexposed'] = unexposed_LBW_prevalence
  result['LBW_exposed'] = exposed_LBW_prevalence
  result_df = pd.DataFrame(result) 
  return (result_df)

# COMMAND ----------

basic_columns = ['ob_hx_infant_sex', 'ethnic_group', 'age_at_start_dt', 'race_group', 'Parity', 'Gravidity', 'insurance', 'pregravid_bmi', 'Preterm_history', 'gestational_days']
basic_binary_columns = ['smoker', 'illegal_drug_user', 'alcohol_user']
comorbidity_binary_columns = ['prepregnancy_chronic_kidney_disease','prepregnancy_diabetes','prepregnancy_leukemia','prepregnancy_pneumonia','prepregnancy_sepsis','prepregnancy_cardiovascular_diseases','prepregnancy_anemia','prepregnancy_sickle_cell_diseases','prepregnancy_cystic_fibrosis', 'prepregnancy_asthma']
prenatal_diagnoses_binary_columns = [ 'prenatal_history_csec','prenatal_diabetes','prenatal_preterm_labor', 'prenatal_anemia', 'prenatal_bacterial_infection', 'prenatal_rhd_negative','prenatal_breech_presentation','prenatal_abnormal_fetal_movement','prenatal_hypertensive_disorder', 'prenatal_poor_fetal_growth','prenatal_placenta_previa','prenatal_anxiety_depression','prenatal_excessive_fetal_growth','prenatal_prom',  'prenatal_hypertensive_disorder']
prepregnancy_diagnoses_binary_columns = ['prepregnancy_diabetes','prepregnancy_anemia', 'prepregnancy_bacterial_infection',  'prepregnancy_vitamin_d_deficiency', 'prepregnancy_gerd','prepregnancy_irregular_period_codes', 'prepregnancy_fatigue', 'prepregnancy_iud','prepregnancy_asthma','prepregnancy_anxiety_depression','prepregnancy_hypertensive_disorder', 'prepregnancy_hypothyroidism',  'prepregnancy_insomina']
med_list = list(set(op_total.columns)-set(intersection(op_total.columns, mom_diagnoses2.columns)))
columns_including = ['PTB_category', 'SGA_category', 'LBW_category']

# COMMAND ----------

basic_covariate = list(set(full_covariate)-set(comorbidity_binary_columns+prenatal_diagnoses_binary_columns+prepregnancy_diagnoses_binary_columns))
comorbidity_covariate = basic_covariate + comorbidity_binary_columns
SA_covariate = basic_covariate + prenatal_diagnoses_binary_columns + prepregnancy_diagnoses_binary_columns

# COMMAND ----------

for i in basic_covariate:
  if i not in full_covariate:
    print (i)
print ('basic covariate search completed')
for i in comorbidity_covariate:
  if i not in full_covariate:
    print (i)
print ('comorbidity covariate search completed')
for i in SA_covariate:
  if i not in full_covariate:
    print (i)
print ('SA covariate search completed')

# COMMAND ----------

med_list = [med for med in med_list if med != 'admission_datetime_filled']

# COMMAND ----------

# unadjusted 
# get_unadjusted_outcome(mom_diagnoses_op_total_pd, med_list)

# COMMAND ----------

# import numpy as np
# basic_op_total_psm_dict = run_PSM(mom_diagnoses_op_total_pd, med_list, basic_covariate)

# COMMAND ----------

# !pip install pingouin

# COMMAND ----------

len(basic_op_total_psm_dict.keys())

# COMMAND ----------

#  import pingouin as pg
#  basic_op_total_psm_outcome = get_PSM_outcome(basic_op_total_psm_dict, basic_covariate)
#  basic_op_total_psm_outcome

# COMMAND ----------

# comorbidity_op_total_psm_dict = run_PSM(mom_diagnoses_op_total_pd, med_list, comorbidity_covariate)

# COMMAND ----------

matched = comorbidity_op_total_psm_dict['erythromycin']
x = matched[matched['erythromycin']==1]['prepregnancy_chronic_kidney_disease']
y = matched[matched['erythromycin']==0]['prepregnancy_chronic_kidney_disease']

# COMMAND ----------

# comorbidity_op_total_psm_outcome = get_PSM_outcome(comorbidity_op_total_psm_dict, comorbidity_covariate)
# comorbidity_op_total_psm_outcome

# COMMAND ----------

# SA_op_total_psm_dict = run_PSM(mom_diagnoses_op_total_pd, med_list, SA_covariate)

# COMMAND ----------

SA_covariate

# COMMAND ----------

def calculate_effectsize(matched_df, group, variables):
  efts = []
  for v in variables:
    matched_df = matched_df
    x = matched_df[matched_df[group]==1].loc[:,v]
    y = matched_df[matched_df[group]==0].loc[:,v]
    eft = pg.compute_effsize(x, y, eftype='cohen')
    import math
    if math.isnan (eft):
      eft = 0
    efts.append(eft)
  return (np.nanmean(np.array(efts)))

# COMMAND ----------

# SA_op_total_psm_outcome = get_PSM_outcome(SA_op_total_psm_dict, SA_covariate)
# SA_op_total_psm_outcome