# Author: Yeon Mi Hwang  
# Date: 01/15/2024
# Disclaimer : This code was originally written in databricks workspace. 
# Objective of A_get_indication.py : To get indication of acyclovir
# Workflow of A_get_indication.py
## 1. load necessary packages and functions 
## 2. get indication 

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
# 2. get indication

mom = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_2022")

indication_interest = ['shingles','genital_herpes','chicken_pox','cold_sore','herpes_simplex','hsv1', 'hsv2', 'primary_gh', 'recurrent_gh']

get_problem_diagnosis(cohort_df=mom,
                         filter_string = "date_of_entry <= ob_delivery_delivery_date",
                         add_cc_columns = indication_interest,
                         save_table_name = 'yh_maternity_problem_a_v_temp_new')


get_encounter_diagnosis(cohort_df=mom,
                         filter_string = "contact_date <= ob_delivery_delivery_date",
                         add_cc_columns = indication_interest,
                         save_table_name = 'yh_maternity_encounter_a_v_temp_new')


mom_diagnoses = add_problems_encounters_conditions(mom,
                                                             add_cc_columns = indication_interest,
                                                             problem_table_name = 'yh_maternity_problem_a_v_temp_new',
                                                             encounter_table_name =  'yh_maternity_encounter_a_v_temp_new')


table_name = 'yh_maternity_hsv_indication_new'
write_data_frame_to_sandbox(mom_diagnoses, table_name, sandbox_db='rdp_phi_sandbox', replace=True)