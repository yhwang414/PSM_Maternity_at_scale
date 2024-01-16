# Author: Yeon Mi Hwang  
# Date: 01/15/2024
# Disclaimer : This code was originally written in databricks workspace. 
# Objective of A_generate_complete_cohort.py : To prepare master cohort dataframe for analysis
# Workflow of A_generate_complete_cohort.py
## 1. load necessary packages and functions 
## 2. load relevant dataframe 
## 3. join dataframes 





# ----------------------------------------------------------------------------
# 1. load necessary packages and functions 

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import *


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)
import CEDA_Tools_v1_1.load_ceda_etl_tools
def intersection(lst1, lst2): 
    return list(set(lst1) & set(lst2)) 

# ----------------------------------------------------------------------------
# 2. load relevant dataframe 

## 2.1 load medication dataframe 

spark.sql("REFRESH TABLE rdp_phi_sandbox.yh_maternity_meds_op_total_new")
spark.sql("REFRESH TABLE rdp_phi_sandbox.yh_maternity_meds_ip_total_new")
spark.sql("REFRESH TABLE rdp_phi_sandbox.yh_maternity_meds_op_1_new")
spark.sql("REFRESH TABLE rdp_phi_sandbox.yh_maternity_meds_ip_1_new")
spark.sql("REFRESH TABLE rdp_phi_sandbox.yh_maternity_meds_total_new")
spark.sql("REFRESH TABLE rdp_phi_sandbox.yh_maternity_meds_total_1_new")
op_total = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_meds_op_total_new")
ip_total = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_meds_ip_total_new")
total = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_meds_total_new")
op_1 = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_meds_op_1_new")
ip_1 = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_meds_ip_1_new")
total_1 = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_meds_total_1_new")

## 2.2 load cohort dataframe

moms = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_2022")


## 2.3 load diagnosis dataframe
prepregnancy_diagnoses = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_2022_prepregnancy_diagnosis")
prenatal_diagnoses = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_2022_prenatal_diagnosis")


# ----------------------------------------------------------------------------
# 3. join dataframes


mom_diagnoses1 = moms.join(prepregnancy_diagnoses, intersection(moms.columns, prepregnancy_diagnoses.columns), 'left')
mom_diagnoses2 = mom_diagnoses1.join(prenatal_diagnoses, intersection(mom_diagnoses1.columns, prenatal_diagnoses.columns), 'left')
columns_drop = list(set(intersection(mom_diagnoses2.columns, total_1.columns))-set(['instance','pat_id','lmp']))
mom_diagnoses_op_total = mom_diagnoses2 .join(op_total.drop(*columns_drop), ['instance','pat_id','lmp'], 'left')
# mom_diagnoses_ip_total = mom_diagnoses.join(ip_total.drop(*columns_drop), ['instance','pat_id','lmp'], 'left')
mom_diagnoses_op_1 = mom_diagnoses2.join(op_1.drop(*columns_drop), ['instance','pat_id','lmp'], 'left')
# mom_diagnoses_ip_1 = mom_diagnoses.join(ip_1.drop(*columns_drop), ['instance','pat_id','lmp'], 'left')
mom_diagnoses_total_1 = mom_diagnoses2.join(total_1.drop(*columns_drop), ['instance','pat_id','lmp'], 'left')
mom_diagnoses_total = mom_diagnoses2.join(total.drop(*columns_drop), ['instance','pat_id','lmp'], 'left')



write_data_frame_to_sandbox(mom_diagnoses_op_total, "yh_maternity_meds_diagnoses_op_2022", sandbox_db='rdp_phi_sandbox', replace=True)



write_data_frame_to_sandbox(mom_diagnoses_op_1, "yh_maternity_meds_diagnoses_op_1_2022", sandbox_db='rdp_phi_sandbox', replace=True)