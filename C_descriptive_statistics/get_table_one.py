# Author: Yeon Mi Hwang  
# Date: 01/15/2024
# Disclaimer : This code was originally written in databricks workspace. 
# Objective get_table_one.py : get table one 
# Workflow of get_table_one.py
## 1. load necessary packages and functions 
## 2. load dataframes
## 3. generate table one 

# ----------------------------------------------------------------------------
# 1. load necessary packages and functions 
import CEDA_Tools_v1_1.load_ceda_etl_tools
import numpy as np
import pandas as pd
from tableone import TableOne



def intersection(lst1, lst2):
    lst3 = [value for value in lst1 if value in lst2]
    return lst3

# ----------------------------------------------------------------------------
# 2. load dataframes 

moms = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_2022")
prepregnancy_diagnoses = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_2022_prepregnancy_diagnosis")
prenatal_diagnoses = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_2022_prenatal_diagnosis")
mom_diagnoses1 = moms.join(prepregnancy_diagnoses, intersection(moms.columns, prepregnancy_diagnoses.columns), 'left')
mom_diagnoses2 = mom_diagnoses1.join(prenatal_diagnoses, intersection(mom_diagnoses1.columns, prenatal_diagnoses.columns), 'left')


write_data_frame_to_sandbox(mom_diagnoses2, "yh_maternity_diagnoses_2022", sandbox_db='rdp_phi_sandbox', replace=True)


maternity_2013 = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_diagnoses_2022")

maternity_2013 = maternity_2013.dropDuplicates(['pat_id', 'instance', 'lmp', 'gestational_days', 'ob_hx_infant_sex'])

# ----------------------------------------------------------------------------
# 3. generate table one


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

from pyspark.sql.functions import col, create_map, lit
from itertools import chain

mapping = {'SGA': 1, 'nonSGA': 0, 'LBW': 1, 'nonLBW': 0, 'preterm': 1, 'term':0}
mapping_expr = create_map([F.lit(x) for x in chain(*mapping.items())])

def convert_df(df):
  df = df.withColumn('delivery_year', F.year(F.col('ob_delivery_delivery_date')))
  df = df.withColumn('alcohol_user', F.col('alcohol_user').cast(StringType()))
  df = df.withColumn('smoker', F.col('smoker').cast(StringType()))
  df = df.withColumn('illegal_drug_user', F.col('illegal_drug_user').cast(StringType()))
  df = df.withColumn('pregravid_bmi', F.col('pregravid_bmi').cast(FloatType()))
  df = df.withColumn('Preterm_history', F.col('Preterm_history').cast(StringType()))
  df = df.withColumn('Parity', F.col('Parity').cast(StringType()))
  df = df.withColumn('Gravidity', F.col('Gravidity').cast(StringType()))
  df = df.withColumn('PTB_category', F.col("preterm_category"))
  df = df.withColumn('SGA_category', F.col("SGA"))
  df = df.withColumn('LBW_category', F.col("LBW"))
  return df

# COMMAND ----------

maternity_2013 = convert_df(maternity_2013)

# COMMAND ----------

maternity_2013.count()

# COMMAND ----------


SVI_score = ['RPL_THEMES', #overall tract summary ranking variable 
             'RPL_THEME1', #socioeconomic ranking variable 
             'RPL_THEME2', #household composition and disability 
             'RPL_THEME3', #minority status and language 
             'RPL_THEME4']  #housing type and transportation 

ruca_col = ['SecondaryRUCACode2010']


maternity_2013 = add_geo_features(maternity_2013, 'svi2018_us', join_cols = ['pat_id', 'instance']).select(*(maternity_2013.columns + SVI_score))
maternity_2013= add_geo_features(maternity_2013, 'ruca2010revised', join_cols = ['pat_id', 'instance']).select(*(maternity_2013.columns + ruca_col))
maternity_2013 = maternity_2013.withColumn('ruca_categorization', categorize_ruca_udf(F.col('SecondaryRUCACode2010')))


for svi in SVI_score:
  maternity_2013 = maternity_2013.withColumn(svi, F.col(svi).cast(FloatType())).withColumn(svi, F.when(F.col(svi)<0, None).otherwise(F.col(svi)))

# COMMAND ----------


maternity_2013 = maternity_2013.fillna('Missing', subset=['BMI_category', 'race_group', 'insurance', 'delivery_method', 'Gravidity', 'Parity', 'ruca_categorization', 'smoker', 'illegal_drug_user', 'alcohol_user'])





basic_columns = ['delivery_year','ob_hx_infant_sex', 'ethnic_group', 'age_at_start_dt', 'race_group', 'Parity', 'Gravidity', 'insurance',  'Preterm_history', 'gestational_days',  'RPL_THEMES', 'RPL_THEME1', 'RPL_THEME2', 'RPL_THEME3', 'RPL_THEME4', 'ruca_categorization', 'BMI_category', 'delivery_method', 'age_group', 'smoker', 'illegal_drug_user', 'alcohol_user']
comorbidity_binary_columns = ['prepregnancy_chronic_kidney_disease','prepregnancy_diabetes','prepregnancy_leukemia','prepregnancy_pneumonia','prepregnancy_sepsis','prepregnancy_cardiovascular_diseases','prepregnancy_anemia','prepregnancy_sickle_cell_diseases','prepregnancy_cystic_fibrosis', 'prepregnancy_asthma']
prenatal_diagnoses_binary_columns = [ 'prenatal_history_csec','prenatal_diabetes','prenatal_preterm_labor', 'prenatal_anemia', 'prenatal_bacterial_infection', 'prenatal_rhd_negative','prenatal_breech_presentation','prenatal_abnormal_fetal_movement','prenatal_hypertensive_disorder', 'prenatal_poor_fetal_growth','prenatal_placenta_previa','prenatal_anxiety_depression','prenatal_excessive_fetal_growth','prenatal_prom',  'prenatal_hypertensive_disorder']
prepregnancy_diagnoses_binary_columns = ['prepregnancy_diabetes','prepregnancy_anemia', 'prepregnancy_bacterial_infection',  'prepregnancy_vitamin_d_deficiency', 'prepregnancy_gerd','prepregnancy_irregular_period_codes', 'prepregnancy_fatigue', 'prepregnancy_iud','prepregnancy_asthma','prepregnancy_anxiety_depression','prepregnancy_hypertensive_disorder', 'prepregnancy_hypothyroidism',  'prepregnancy_insomina']
columns_including = ['PTB_category', 'SGA_category', 'LBW_category']

# COMMAND ----------

select_columns = list(set(basic_columns + comorbidity_binary_columns +prenatal_diagnoses_binary_columns + prepregnancy_diagnoses_binary_columns + columns_including))

# COMMAND ----------

maternity_2013 = maternity_2013.select(*select_columns)

# COMMAND ----------

continuous_columns = ['gestational_days', 'age_at_start_dt', 'RPL_THEMES', 'RPL_THEME1', 'RPL_THEME2', 'RPL_THEME3', 'RPL_THEME4']

characteristic_columns = ['age_group', 'delivery_year', 'race_group','ethnic_group','BMI_category','ob_hx_infant_sex', 'delivery_method', 'insurance', 'smoker', 'illegal_drug_user', 'alcohol_user', 'ruca_categorization', 'Gravidity', 'Parity',  'Preterm_history']

comorbidity_binary_columns = list(set(columns_including +comorbidity_binary_columns + prenatal_diagnoses_binary_columns + prepregnancy_diagnoses_binary_columns))



# COMMAND ----------

def format_gravidity(gravidity):
  if gravidity is None:
    return None
  elif gravidity == 'Missing':
    return 'Missing'
  elif gravidity == '1':
    return '1'
  elif gravidity in ['2', '3', '4']:
    return '2-4'
  else:
    return '5≤'
def format_parity(parity):
  if parity is None:
    return None
  elif parity == 'Missing':
    return 'Missing'
  elif parity == '0':
    return '0'
  elif parity == '1':
    return '1'
  elif parity in ['2', '3', '4']:
    return '2-4'
  else:
    return '5≤'

def format_preterm_history(history, parity):
  if parity is None:
    return None
  elif parity == 'Missing':
    return 'Missing'
  elif history == '0':
    return 'No'
  else:
    return 'Yes'
  
def format_insurance(insurance):
  if insurance == 'Commercial':
    return 'Commercial'
  elif insurance in ['Medicaid', 'Medicare']:
    return 'Medicaid/Medicare'
  elif insurance == 'Uninsured-Self-Pay':
    return 'Self Pay'
  else:
    return 'Missing'

def format_table_one(pd):
  return_pd = pd  
  return_pd['delivery_year'] = return_pd['delivery_year'].map(str)
  for index, row in return_pd.iterrows():
    return_pd.at[index, 'Gravidity'] = format_gravidity(row['Gravidity'])
    return_pd.at[index, 'Parity'] = format_parity(row['Parity'])
    return_pd.at[index, 'Preterm_history'] = format_preterm_history(row['Preterm_history'], row['Parity'])
    return_pd.at[index, 'insurance'] = format_insurance(row['insurance'])
  return return_pd

   

# COMMAND ----------

maternity_2013_pd = maternity_2013.toPandas()

# COMMAND ----------

maternity_2013_pd2 = format_table_one(maternity_2013_pd)

# COMMAND ----------

categorical_column_category_dict = {
  'ob_hx_infant_sex' : ['Female', 'Male', 'Unknown', 'Other'],
  'age_group' : ['17 or younger', '18-24', '25-29', '30-34', '35-39', '40 or older'], 
  'delivery_year' : ['2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022'], 
  'race_group' : ['American Indian or Alaska Native',  'Asian' , 'Black or African American', 'Native Hawaiian or Other Pacific Islander', 'White or Caucasian', 'Multirace', 'Other', 'Missing'],
  'ethnic_group' : ['Hispanic_Latino', 'Not_Hispanic_Latino', 'Unknown_NotReported'],
  'BMI_category' : ['underweight', 'normal', 'obese', 'overweight', 'Missing'],
  'delivery_method' : ['vaginal', 'c-section', 'other', 'Missing'], 
  'insurance' : ['Commercial', 'Medicaid/Medicare', 'Self Pay', 'Missing'],
  'ruca_categorization' : ['Metropolitan', 'Micropolitan', 'SmallTown', 'Rural', 'Missing'], 
  'Parity': ['0','1','2-4','5≤','Missing'],
  'Gravidity': ['1','2-4','5≤','Missing'],
  'Preterm_history' : ['Yes','No','Missing'],
  'PTB_category' : ['term', 'preterm'], 
  'SGA_category' : ['nonSGA', 'SGA'],
  'LBW_category' : ['nonLBW', 'LBW'],
  'alcohol_user' : ['1','0','Missing'],
  'illegal_drug_user' : ['1','0','Missing'],
  'smoker' : ['1','0','Missing']
} 

# COMMAND ----------


categorical_columns = ['age_group', 'delivery_year', 'race_group','ethnic_group','BMI_category','ob_hx_infant_sex', 'delivery_method', 'insurance', 'Gravidity', 'Parity', 'Preterm_history', 'ruca_categorization','smoker', 'illegal_drug_user', 'alcohol_user', 'PTB_category', 'SGA_category', 'LBW_category']
binary_columns = [i for i in characteristic_columns  + comorbidity_binary_columns   if i not in categorical_columns]
continuous_columns = ['gestational_days', 'age_at_start_dt', 'RPL_THEMES', 'RPL_THEME1', 'RPL_THEME2', 'RPL_THEME3', 'RPL_THEME4']

# COMMAND ----------

columns = ['gestational_days', 'PTB_category', 'SGA_category', 'LBW_category', 'age_at_start_dt', 'age_group',  'race_group','ethnic_group','BMI_category', 'insurance', 'smoker', 'illegal_drug_user', 'alcohol_user', 'RPL_THEMES', 'RPL_THEME1', 'RPL_THEME2', 'RPL_THEME3', 'RPL_THEME4', 'ruca_categorization','delivery_year', 'ob_hx_infant_sex', 'Gravidity', 'Parity', 'Preterm_history', 'delivery_method'] + binary_columns

# COMMAND ----------

def generate_categorical_table_one(pd, column, category_list):
  new_dict  = {key: 0 for key in category_list}
  for item in pd[column]:
    for c in category_list:
      if item == c:
        new_dict[c] += 1 
  return new_dict

def category_str_values(category_values):
  str_category_values = []
  for v in category_values:
    percentage = '('+str(round(v/sum(category_values)*100,1))+')'
    new_v = str(v) + ' ' + percentage
    str_category_values.append(new_v)
  return (str_category_values)

def get_table_dict (pd, name, columns = columns, categorical_columns = categorical_columns, continuous_columns = continuous_columns, binary_columns = columns, category_list = categorical_column_category_dict):
  table_dict ={}
  pd2 = pd[columns]
  total_n = len(pd)
  variables = []
  values = []
  for col in columns:
    print (col)
    if col in categorical_columns: 
      category_dict = generate_categorical_table_one(pd, col, category_list[col])  
      category_index = list(category_dict.keys()) 
      category_values = category_str_values(list(category_dict.values()))
      variables.append(col)
      values.append('')
      variables += category_index
      values += category_values
    elif col in continuous_columns:
        variables.append(col)
        mean = str(round(pd2[col].median(),1))
        sd = str(round(pd2[col].std(),1))
        str_val = mean +' (' + sd + ')'
        values.append(str_val)
    elif col in binary_columns:
      pd2[col] = pd2[col].astype(int)
      count = sum(pd2[col])
      str_count = str(count)
      string = str_count + ' (' + str(round(count/len(pd2)*100, 1)) + ')'
      variables.append(col)
      values.append(string)
  table_dict['variable'] = variables 
  table_dict[name] = values
  return table_dict


  

# COMMAND ----------

table_one_dict = get_table_dict(maternity_2013_pd2, 'overall')
table_one_df = pd.DataFrame.from_dict(table_one_dict)

# COMMAND ----------

# final_table_one = table_one_df
# final_table_one

# COMMAND ----------

categorical_column_category_dict = {
  'ob_hx_infant_sex' : ['Female', 'Male', 'Unknown', 'Other'],
  'age_group' : ['17 or younger', '18-24', '25-29', '30-34'], 
  'delivery_year' : ['2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022'], 
  'race_group' : ['American Indian or Alaska Native',  'Asian' , 'Black or African American', 'Native Hawaiian or Other Pacific Islander', 'White or Caucasian', 'Multirace', 'Other', 'Missing'],
  'ethnic_group' : ['Hispanic_Latino', 'Not_Hispanic_Latino', 'Unknown_NotReported'],
  'BMI_category' : ['underweight', 'normal', 'obese', 'overweight', 'Missing'],
  'delivery_method' : ['vaginal', 'c-section', 'other', 'Missing'], 
  'insurance' : ['Commercial', 'Medicaid/Medicare', 'Self Pay', 'Missing'],
  'ruca_categorization' : ['Metropolitan', 'Micropolitan', 'SmallTown', 'Rural', 'Missing'], 
  'Parity': ['0','1','2-4','5≤','Missing'],
  'Gravidity': ['1','2-4','5≤','Missing'],
  'Preterm_history' : ['Yes','No','Missing'],
  'preterm_category' : ['term', 'preterm'], 
  'SGA' : ['nonSGA', 'SGA'],
  'LBW' : ['nonLBW', 'LBW']
} 

# COMMAND ----------

def make_binary_outcome_dict(df, binary_column):
  d = {'Yes': 0, 'No': 0}
  for index, row in df.iterrows():
    binary = row[binary_column]
    if binary == 1:
      d['Yes'] += 1
    else:
      d['No'] += 1
  return d


# COMMAND ----------

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

# COMMAND ----------

def get_fisher(treatment_dict, control_dict):
  from scipy.stats import fisher_exact
  contingency_table = [list(treatment_dict.values()), list(control_dict.values())]
  fisher_result = fisher_exact(contingency_table)
  pval = fisher_result[1]
  return pval

def get_chi(treatment_dict, control_dict):
  from scipy.stats import chi2_contingency
  contingency_table = [list(treatment_dict.values()), list(control_dict.values())]
  chi_result = chi2_contingency(contingency_table)
  pval = chi_result[1]
  return pval

def get_mann_u(treatment_column, control_column):
  from scipy.stats import mannwhitneyu
  pval = mannwhitneyu(treatment_column.dropna(), control_column.dropna(), method="auto")[1]
  return pval

# COMMAND ----------

def get_stat (control_pd, treatment_pd, name, columns = columns, categorical_columns = categorical_columns, continuous_columns = continuous_columns, binary_columns = columns, category_list = categorical_column_category_dict):
  table_dict ={}
  control_pd2 = control_pd[columns]
  treatment_pd2 = treatment_pd[columns]
  variables = []
  values = []
  for col in columns:
    print (col)
    if col in categorical_columns: 
      control_category_dict = generate_categorical_table_one(control_pd2, col, category_list[col])  
      treatment_category_dict = generate_categorical_table_one(treatment_pd2, col, category_list[col])  
      pval = get_chi(treatment_category_dict, control_category_dict)
      variables.append(col)
      values.append(pval)
    elif col in continuous_columns:
      variables.append(col)
      treatment_column = treatment_pd2[col]
      control_column = control_pd2[col]
      pval = get_mann_u(treatment_column, control_column)
      values.append(pval)
    elif col in binary_columns:
      control_binary_dict = make_binary_outcome_dict(control_pd2, col)  
      treatment_binary_dict = make_binary_outcome_dict(treatment_pd2, col) 
      variables.append(col)
      pval = get_fisher(treatment_binary_dict, control_binary_dict)
      values.append(pval)
  table_dict['variable'] = variables 
  table_dict[name] = values
  return table_dict

# COMMAND ----------


categorical_columns = ['age_group', 'delivery_year', 'race_group','ethnic_group','BMI_category','ob_hx_infant_sex', 'delivery_method', 'insurance', 'Gravidity', 'Parity', 'Preterm_history', 'ruca_categorization']
binary_columns = [i for i in characteristic_columns + comorbidity_columns  if i not in categorical_columns]
continuous_columns = ['age_at_start_dt', 'RPL_THEMES', 'RPL_THEME1', 'RPL_THEME2', 'RPL_THEME3', 'RPL_THEME4']

# COMMAND ----------

columns = ['age_at_start_dt', 'age_group', 'race_group','ethnic_group','BMI_category', 'insurance', 'smoker', 'illegal_drug_user', 'alcohol_user', 'RPL_THEMES', 'RPL_THEME1', 'RPL_THEME2', 'RPL_THEME3', 'RPL_THEME4', 'ruca_categorization',  'Gravidity', 'Parity', 'Preterm_history', 'ob_hx_infant_sex', 'delivery_year',"diabetes_type1and2", "chronic_kidney_disease", "obesity", "chronic_liver_disease",
"asthma", "HIV", "chronic_lung_disease", "depression", "hypercoagulability", "pneumonia", "urinary_tract_infection", "sexually_transmitted_disease", "periodontitis_disease", "cardiovascular_disease", "sickle_cell_disease", "sepsis", 
## Removed list: 'uveitis'
'ibd', 'rheumatoid_arthritis', 'multiple_sclerosis', 'psoriatic_arthritis', 'psoriasis', 'systemic_sclerosis', 'spondyloarthritis', 'systemic_lupus', 'vasculitis','sarcoidosis', 'APS', 'sjogren_syndrome']

# COMMAND ----------

IMID_table_stat_dict = get_stat (noIMID_pd2, IMID_pd2, 'IMID', columns = columns, categorical_columns = categorical_columns, continuous_columns = continuous_columns, binary_columns = columns, category_list = categorical_column_category_dict)
IMID_table_stat_df = pd.DataFrame.from_dict(IMID_table_stat_dict)

# COMMAND ----------

table_stat_dict = {}
for imid in imid_list:
  imid_stat = get_stat(noIMID_pd2, IMID_pd_df_dict[imid], imid, columns = columns, categorical_columns = categorical_columns, continuous_columns = continuous_columns, binary_columns = columns, category_list = categorical_column_category_dict)
  table_stat_dict[imid] = pd.DataFrame.from_dict(imid_stat)

# COMMAND ----------

final_table_stat = IMID_table_stat_df
for imid in imid_list:
  final_table_stat[imid] = table_stat_dict[imid][imid] 
final_table_stat

# COMMAND ----------

sparkDF=spark.createDataFrame(final_table_stat) 

# COMMAND ----------

write_data_frame_to_sandbox(sparkDF, "yh_IMID_tableone_stat", "rdp_phi_sandbox")

# COMMAND ----------

tableone_stat = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_IMID_tableone_stat")

# COMMAND ----------



# COMMAND ----------

new_imid_list = ['IMID', 'ibd', 'rheumatoid_arthritis', 'multiple_sclerosis', 'psoriatic_arthritis', 'psoriasis', 'systemic_sclerosis', 'spondyloarthritis', 'systemic_lupus', 'vasculitis','sarcoidosis', 'APS', 'sjogren_syndrome']


# COMMAND ----------

from statsmodels.stats.multitest import multipletests
tableone_stat_pd = tableone_stat.toPandas()


# COMMAND ----------

variables = ['age_at_start_dt', 'age_group', 'race_group','ethnic_group','BMI_category', 'insurance', 'smoker', 'illegal_drug_user', 'alcohol_user', 'RPL_THEMES', 'RPL_THEME1', 'RPL_THEME2', 'RPL_THEME3', 'RPL_THEME4', 'ruca_categorization',  'Gravidity', 'Parity', 'Preterm_history', 'ob_hx_infant_sex', 'delivery_year',"diabetes_type1and2", "chronic_kidney_disease", "obesity", "chronic_liver_disease",
"asthma", "HIV", "chronic_lung_disease", "depression", "hypercoagulability", "pneumonia", "urinary_tract_infection", "sexually_transmitted_disease", "periodontitis_disease", "cardiovascular_disease", "sickle_cell_disease", "sepsis"]

# COMMAND ----------

tableone_reset = tableone_stat_pd.set_index('variable')

# COMMAND ----------

tableone_reset = tableone_reset.loc[variables]

# COMMAND ----------

tableone_reset2 = tableone_reset
tableone_reset3 = tableone_reset
for imid in new_imid_list:
  tableone_reset2[imid] = multipletests(tableone_reset[imid], method = 'fdr_bh')[1]

# COMMAND ----------

# tableone_reset2

# COMMAND ----------

for v in variables:
  for imid in new_imid_list:  
    p = tableone_reset2.loc[v, imid]
    tableone_reset3.loc[v, imid] =    get_pval_signal(p)

# COMMAND ----------

# tableone_reset3