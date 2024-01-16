# Author: Yeon Mi Hwang  
# Date: 01/15/2024
# Disclaimer : This code was originally written in databricks workspace. 
# Objective of A_get_indication.py : To get indication of ferrous sulfate 
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


mom = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_2022")

# ----------------------------------------------------------------------------
# 2. get indication
indication_interest = ['anemia', 'iron_x_anemia']

# COMMAND ----------

get_problem_diagnosis(cohort_df=mom,
                         filter_string = "date_of_entry <= lmp AND date_sub(lmp, 180) < date_of_entry",
                         add_cc_columns = indication_interest,
                         save_table_name = 'yh_maternity_problem_anemia_temp_new_062423')

# COMMAND ----------

get_encounter_diagnosis(cohort_df=mom,
                         filter_string = "contact_date <= lmp AND date_sub(lmp, 180) < contact_date",
                         add_cc_columns = indication_interest,
                         save_table_name = 'yh_maternity_encounter_anemia_temp_new_062423')

# COMMAND ----------

mom_diagnoses = add_problems_encounters_conditions(mom,
                                                             add_cc_columns = indication_interest,
                                                             problem_table_name = 'yh_maternity_problem_anemia_temp_new_062423',
                                                             encounter_table_name =  'yh_maternity_encounter_anemia_temp_new_062423')

# COMMAND ----------

mom_anemia = mom_diagnoses.withColumnRenamed('anemia', 'prepregnancy_anemia')

# COMMAND ----------

table_name = 'yh_maternity_prepregnancy_anemia_062423'
write_data_frame_to_sandbox(mom_anemia, table_name, sandbox_db='rdp_phi_sandbox', replace=True)

# COMMAND ----------

get_problem_diagnosis(cohort_df=mom,
                         filter_string = " lmp <= date_of_entry AND date_of_entry < ob_delivery_delivery_date",
                         add_cc_columns = indication_interest,
                         save_table_name = 'yh_maternity_problem_prenatal_anemia_temp_new')

# COMMAND ----------

get_encounter_diagnosis(cohort_df=mom,
                         filter_string = "lmp <= contact_date AND contact_date < ob_delivery_delivery_date",
                         add_cc_columns = indication_interest,
                         save_table_name = 'yh_maternity_encounter_prenatal_anemia_temp_new')

# COMMAND ----------

mom_diagnoses = add_problems_encounters_conditions(mom,
                                                             add_cc_columns = indication_interest,
                                                             problem_table_name = 'yh_maternity_problem_prenatal_anemia_temp_new',
                                                             encounter_table_name =  'yh_maternity_encounter_prenatal_anemia_temp_new')

# COMMAND ----------

mom_anemia = mom_diagnoses.withColumnRenamed('anemia', 'prenatal_anemia')

# COMMAND ----------

table_name = 'yh_maternity_prenatal_anemia'
write_data_frame_to_sandbox(mom_anemia, table_name, sandbox_db='rdp_phi_sandbox', replace=True)