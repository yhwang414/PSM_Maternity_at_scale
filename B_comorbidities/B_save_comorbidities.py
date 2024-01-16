# Author: Yeon Mi Hwang  
# Date: 01/15/2024
# Disclaimer : This code was originally written in databricks workspace. 
# Objective of B_save_comorbidities.py : generate dataframe with features of comorbidities 
# Workflow of B_save_comorbidities.py 
## 1. load necessary packages and functions 
## 2. load cohort 
## 3. load and save diagnosis 

# ----------------------------------------------------------------------------
# 1. load necessary packages and functions 
import CEDA_Tools_v1_1.load_ceda_etl_tools
from datetime import date, datetime, timedelta
from dateutil.relativedelta import *
import pyspark.sql.functions as F
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import scipy
import sklearn 
%matplotlib inline
from scipy import stats
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import NearestNeighbors
from sklearn import datasets, linear_model
from sklearn import metrics
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import KFold, cross_val_score
from sklearn.utils import shuffle
import math
plt.rcParams.update({'font.size': 12})
plt.rcParams['pdf.fonttype'] = 42
spark.conf.set('spark.sql.execution.arrow.enabled', False)

# ----------------------------------------------------------------------------
# 2. load cohort
moms = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_2022")


# ----------------------------------------------------------------------------
# 3. load diagnosis
def get_problem_diagnosis(cohort_df=None,
                         filter_string = "date_of_entry >= lmp AND date_of_entry < ob_delivery_delivery_date",
                         add_cc_columns = cc_list, 
                         save_table_name = 'yh_maternity_problem_new_temp'):
    """Get conditions status based on specified filter criteria
  
    Parameters:
    cohort_df (PySpark df): Optional cohort dataframe for which to get problem list records (
                          get all patients in the RDP). This dataframe *must* include pat_id, instance, lmp, ob_delivery_delivery_date columns for joining
    include_cohort_columns (list): List of columns in cohort dataframe to keep in results. Default is
                                 None (i.e. include only pat_id, instance, lmp, ob_delivery_delivery_date from cohort
                                 dataframe for joining)
    add_cc_columns (list): List of medication cc columns to add (e.g. 'asthma',
                         'cardiac_arrhythmia', 'chronic_lung_disease')
  
    Returns:
    PySpark df: Dataframe containing condition status satisfying specified filter criteria
    """
    conditions = add_cc_columns
    spark.sql("REFRESH TABLE rdp_phi_sandbox.yh_dx_id_cc_label_mapping_dissertation")
    cc_mapping_df = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_dx_id_cc_label_mapping_dissertation")
    if 'name' in cohort_df.columns:
      cohort_df = cohort_df.drop('name')
    # get problem 
    from pyspark.sql.functions import when
    
    problem_table_name = 'hadlock_problem_list'
    problem_records_df = spark.sql("SELECT * FROM rdp_phi_sandbox.{table}".format(table=problem_table_name))
    
    problem_records_df = problem_records_df.join(cc_mapping_df, ['diagnosis_id'], how='inner')
    
    patient_problem_df = problem_records_df.join(cohort_df, ['patient_id'], how = 'right') \
      .filter(filter_string)\
      .select(['patient_id','lmp','ob_delivery_delivery_date'] + conditions)
    # get active problem list 
    active_patient_problem_df = patient_problem_df.where(F.col('problem_status')=='Active')
    problem_table_name = save_table_name
    write_data_frame_to_sandbox(active_patient_problem_df, problem_table_name, sandbox_db='rdp_phi_sandbox', replace=True)


cohort_df = moms
conditions = cc_list
spark.sql("REFRESH TABLE rdp_phi_sandbox.yh_dx_id_cc_label_mapping_dissertation")
cc_mapping_df = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_dx_id_cc_label_mapping_dissertation")



save_problemlist(cohort_df)
save_encounters(cohort_df)



problem_list = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_2022_problem_list")
encounters = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_2022_encounters")



# prepregnancy encounter diagnosis
save_encounter_diagnosis(cohort_df=cohort_df,
                          patient_encounter_df = encounters,
                         filter_string = "contact_date < lmp",
                         save_table_name = "yh_maternity_2022_prepregnancy_encounter_diagnosis")

# prenatal encounter diagnosis
save_encounter_diagnosis(cohort_df=cohort_df,
                          patient_encounter_df = encounters,
                         filter_string = "contact_date >= lmp AND contact_date < ob_delivery_delivery_date",
                         save_table_name = "yh_maternity_2022_prenatal_encounter_diagnosis")

# prepregnancy problem diagnosis
save_problem_diagnosis(cohort_df=cohort_df,
                         patient_problem_df = problem_list,
                         filter_string = "date_of_entry < lmp",
                         end_date_column = "lmp",
                         save_table_name = "yh_maternity_2022_prepregnancy_problem_diagnosis")

# prenatal problem diagnosis
save_problem_diagnosis(cohort_df=cohort_df,
                         patient_problem_df = problem_list,
                         filter_string = "date_of_entry >= lmp AND date_of_entry < ob_delivery_delivery_date",
                         end_date_column = "ob_delivery_delivery_date",
                         save_table_name = "yh_maternity_2022_prenatal_problem_diagnosis")


prepregnancy_diagnosis = add_problems_encounters_conditions(cohort_df=cohort_df, problem_table_name = "yh_maternity_2022_prepregnancy_problem_diagnosis", encounter_table_name = "yh_maternity_2022_prepregnancy_encounter_diagnosis")
prenatal_diagnosis = add_problems_encounters_conditions(cohort_df=cohort_df, problem_table_name = "yh_maternity_2022_prenatal_problem_diagnosis", encounter_table_name = "yh_maternity_2022_prenatal_encounter_diagnosis")


prepregnancy_new_columns = ['prepregnancy_'+cc for cc in cc_list] 
prenatal_new_columns= ['prenatal_'+cc for cc in cc_list] 


for i in range(0,len(cc_list)):
  prepregnancy_diagnosis = prepregnancy_diagnosis.withColumnRenamed(cc_list[i], prepregnancy_new_columns[i])
  prenatal_diagnosis = prenatal_diagnosis.withColumnRenamed(cc_list[i], prenatal_new_columns[i])


write_data_frame_to_sandbox(prepregnancy_diagnosis, "yh_maternity_2022_prepregnancy_diagnosis", sandbox_db='rdp_phi_sandbox', replace=True)
write_data_frame_to_sandbox(prenatal_diagnosis, "yh_maternity_2022_prenatal_diagnosis", sandbox_db='rdp_phi_sandbox', replace=True)