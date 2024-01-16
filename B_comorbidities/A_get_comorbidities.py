# Author: Yeon Mi Hwang  
# Date: 01/15/2024
# Disclaimer : This code was originally written in databricks workspace. 
# Objective of A_get_comorbidities.py : Load and clean up comorbidities relevant to the analytic cohort
# Workflow of A_get_comorbidities.py 
## 1. load necessary packages 
## 2. load necessary functions 
## 3. load dataframe 
## 4. get 200 most common diagnoses prepregnancy and post-pregnancy 
## 5. get 10 top features for pre-pregnancy and prenatal (curated. same diagnosis with different names were grouped together) 


# ----------------------------------------------------------------------------
# 1-2. load necessary packages and functions 

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
# 3. load datafrmae 

# 3.1 load cohort datafrmae
moms = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_2022")

# 3.1 load diagnois mapping 
mapping= spark.sql("SELECT * FROM rdp_phi_sandbox.yh_diagnosis_id_mapping")

# 3.2 load prenatal problem list table 

during_problem_list = get_problem_list(moms.select(*['pat_id', 'instance', 'lmp', 'ob_delivery_delivery_date','episode_id'])).filter('noted_date > lmp AND noted_date < ob_delivery_delivery_date')
diagnosis_id = spark.sql("SELECT * FROM rdp_phi_sandbox.hadlock_snomed_diagnosis_id_map_2022")
rest_columns = list(set(during_problem_list.columns) - set(['condition_description', 'snomed_code'])) 
during_problem_list = during_problem_list.withColumn("new", F.arrays_zip('condition_description', 'snomed_code'))\
       .withColumn("new", F.explode("new"))\
       .select(*rest_columns, F.col("new.condition_description").alias("condition_description"), F.col("new.snomed_code").alias("snomed_code"))




during_problem_list_concise = during_problem_list.select(*['pat_id', 'instance', 'lmp', 'ob_delivery_delivery_date', 'condition_description', 'snomed_code']).distinct()



during_snomed_count = during_problem_list_concise.groupBy("condition_description","snomed_code").count().sort(F.desc("count"))


# 3.3 load prepregnancy problem list table 

prior_problem_list = get_problem_list(moms.select(*['pat_id', 'instance', 'lmp', 'ob_delivery_delivery_date','episode_id'])).filter('noted_date < lmp')


prior_problem_list = prior_problem_list.withColumn("new", F.arrays_zip('condition_description', 'snomed_code'))\
       .withColumn("new", F.explode("new"))\
       .select(*rest_columns, F.col("new.condition_description").alias("condition_description"), F.col("new.snomed_code").alias("snomed_code"))

# COMMAND ----------

prior_problem_list_concise = prior_problem_list.select(*['pat_id', 'instance', 'lmp', 'ob_delivery_delivery_date', 'condition_description', 'snomed_code']).distinct()

## 4. get 200 most common diagnoses prepregnancy and post-pregnancy 

prior_snomed_count = prior_problem_list_concise.groupBy("condition_description","snomed_code").count().sort(F.desc("count"))



during_snomed_count = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_during_snomed_count")
prior_snomed_count = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_prior_snomed_count")

# COMMAND ----------

# during_snomed_count.count()

# COMMAND ----------

# prior_snomed_count.count()

# COMMAND ----------

# prior_snomed_count.limit(200).toPandas()

# COMMAND ----------

# during_snomed_count.limit(200).toPandas()