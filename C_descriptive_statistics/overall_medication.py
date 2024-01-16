# Author: Yeon Mi Hwang  
# Date: 01/15/2024
# Disclaimer : This code was originally written in databricks workspace. 
# Objective overall_medication.py : get descriptive statistics of medication records 
# Workflow of overall_medication.py
## 1. load necessary packages and functions 
## 2. get figures 



# ----------------------------------------------------------------------------
# 1. load necessary packages and functions 


import pandas as pd
import pyspark.sql.functions as F
import CEDA_Tools_v1_1.load_ceda_etl_tools
# ----------------------------------------------------------------------------
# 2. get figures  

# use of prescribed medication over time - outpatient and inpatient 

med_df_dict = {'med1' : None,
'med1_op' : None,
'med1_ip' : None, 
'med2' : None,
'med2_op' : None,
'med2_ip' : None
}



for m in med_df_dict.keys():
  med_df = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_meds_pivoted_total_{0}".format(m))
  med_df = med_df.withColumn('delivery_year', F.year('ob_delivery_delivery_date')).filter(F.col('delivery_year')>2012).filter(F.col('delivery_year')<2023)
  med_pd = med_df.toPandas()
  med_df_dict[m] = med_pd



# prescription rate over time 


med_dict = {
  'total' : med_df_dict['med2'],
  'ip' : med_df_dict['med2_ip'],
  'op' : med_df_dict['med2_op']
}


df_list = []
exposure_list = []
population_list = []
year_list = []
prevalence_dict = {}
for k in list(med_dict.keys()):
  df = med_dict[k]
  med_df = df.iloc[:,72:1516]
  for year in [2013,2014,2015,2016,2017,2018,2019,2020,2021,2022]:
    year_df = df[df['delivery_year'] == year]
    med_df = year_df.iloc[:,72:1516]
    exposure_list.append(sum(med_df.max(axis=1))) 
    year_list.append(year)
    df_list.append(k)
    population_list.append(len(year_df))
prevalence_dict['year'] = year_list 
prevalence_dict['type'] = df_list 
prevalence_dict['exposure'] = exposure_list
prevalence_dict['population'] = population_list
prevalence_pd = pd.DataFrame.from_dict(prevalence_dict)



prevalence_pd['prevalence rate(%)'] = prevalence_pd['exposure']/prevalence_pd['population']*100
prevalence_pd.loc[prevalence_pd['type']=='total', 'total'] = 'Yes'
prevalence_pd.loc[prevalence_pd['type']!='total', 'total'] = 'No'


import seaborn as sns
sns.set(font_scale=1.5)
sns.set(style = "whitegrid")
sns.set(font="Arial")
sns.set(rc={'figure.figsize':(8,7)})

# COMMAND ----------

from sklearn import datasets, linear_model
from sklearn.linear_model import LinearRegression
import statsmodels.api as sm
from scipy import stats
total_rate = prevalence_pd[prevalence_pd['type']=='total']
Y = total_rate['prevalence rate(%)']
X = total_rate['year']
slope, intercept, r_value, p_value, std_err = stats.linregress(X,Y)
print ('total prescription p value:', p_value)

op_rate = prevalence_pd[prevalence_pd['type']=='op']
Y = op_rate['prevalence rate(%)']
X = op_rate['year']
slope, intercept, r_value, p_value, std_err = stats.linregress(X,Y)
print ('outpatient prescription p value:', p_value)

ip_rate = prevalence_pd[prevalence_pd['type']=='ip']
Y = ip_rate['prevalence rate(%)']
X = ip_rate['year']
slope, intercept, r_value, p_value, std_err = stats.linregress(X,Y)
print ('inpatient prescription p value:', p_value)




# prescription rate over time based on age group 

# 1. maternal age increase 
# 2. prescription rate based on maternal age group 


moms = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_2022")
moms = moms.withColumn('delivery_year', F.year('ob_delivery_delivery_date')).filter(F.col('delivery_year')>2012).filter(F.col('delivery_year')<2023)

moms_pd = moms.toPandas()


moms_pd[["delivery_year", "age_at_start_dt"]] = moms_pd[["delivery_year", "age_at_start_dt"]].apply(pd.to_numeric)



moms_pd['delivery_year_for_plot'] = moms_pd['delivery_year'].replace({2013: 0, 2014: 1, 2015:2, 2016:3, 2017:4, 2018:5, 2019:6, 2020:7, 2021:8, 2022:9})



# moms_pd['delivery_year_for_plot'].value_counts()


# moms_pd['delivery_year_for_plot'].value_counts()



Q1 = moms_pd['age_at_start_dt'].quantile(0.25)
Q3 = moms_pd['age_at_start_dt'].quantile(0.75)
IQR = Q3 - Q1

# identify outliers
threshold = 1.5
outliers = moms_pd[(moms_pd['age_at_start_dt'] < Q1 - threshold * IQR) | (moms_pd['age_at_start_dt'] > Q3 + threshold * IQR)]
moms_pd_no_outliers = moms_pd.drop(outliers.index)
Y = moms_pd_no_outliers ['age_at_start_dt']
X = moms_pd_no_outliers ['delivery_year_for_plot']
slope, intercept, r_value, p_value, std_err = stats.linregress(X,Y)
print (slope, r_value, p_value)
stats.linregress(X,Y)



# moms_pd_no_outliers.groupby('delivery_year').quantile([0.25, 0.5, 0.75])['age_at_start_dt']

# import matplotlib.pyplot as plt
# import numpy as np
# fig, ax = plt.subplots(figsize=(15, 10))

# ax = sns.violinplot(data=moms_pd_no_outliers, x="delivery_year_for_plot", y="age_at_start_dt", split=True, inner="quart",  color="skyblue")
# sns.regplot(data= moms_pd_no_outliers, x="delivery_year_for_plot", y="age_at_start_dt", scatter=False, ax=ax, color = 'red',  line_kws={'linewidth':10 , 'alpha':0.5})
# ax.set(xlabel = 'delivery year', ylabel = 'maternal age(years)')   
# ax.set_xticklabels([2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022])




moms_pd_no_outliers = moms_pd_no_outliers.dropna(subset=['delivery_year', 'age_group'])



# moms_pd_no_outliers.groupby(['delivery_year', 'age_group']).count()



# sns.set_theme(style="white", font_scale=1.5)
# g =sns.histplot(x="delivery_year", hue = 'age_group', data = moms_pd_no_outliers, stat="percent", multiple="fill",  hue_order=['40 or older', '35-39','30-34', '25-29', '18-24', '17 or younger'], discrete = True, element = 'bars')
# sns.move_legend(g, "upper left")
# g.set(xlabel = 'delivery year', ylabel = 'percent(%)')   
# g.tick_params(axis='y', which='both', labelleft=False, labelright=True)
# g.yaxis.set_label_position("right")
# g.legend_.set_title(None)


df_list = []
df = med_dict['total']
exposure_list = []
population_list = []
year_list = []
prevalence_dict = {}
med_df = df.iloc[:,72:1516]
age_group_list = []
for age_group in [ '17 or younger',  '18-24', '25-29', '30-34','35-39', '40 or older']:
  for year in [2013,2014,2015,2016,2017,2018,2019,2020,2021,2022]:
    age_year_df = df[(df['delivery_year'] == year)&(df['age_group'] == age_group)]
    med_df = age_year_df.iloc[:,72:1516]
    exposure_list.append(sum(med_df.max(axis=1))) 
    year_list.append(year)
    age_group_list.append(age_group)
    population_list.append(len(age_year_df))
prevalence_dict['year'] = year_list
prevalence_dict['age_group'] = age_group_list 
prevalence_dict['exposure'] = exposure_list
prevalence_dict['population'] = population_list
prevalence_pd = pd.DataFrame.from_dict(prevalence_dict)



# prevalence_pd['prevalence rate(%)'] = prevalence_pd['exposure']/prevalence_pd['population']*100
# import numpy as np
# import matplotlib.pyplot as plt
# import matplotlib.font_manager
# import seaborn as sns
# sns.set_theme(style="white", font = "Arial", font_scale=1.5)
# plt.ylim([40, 100])
# sns.lineplot(
#   data=prevalence_pd,
#   x="year", y="prevalence rate(%)", hue="age_group", style="age_group",
#   markers=True, dashes=False, palette = 'colorblind'
# )
# plt.xticks([2013,2014,2015,2016,2017,2018,2019,2020,2021,2022])
# plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.,  frameon=False)
# plt.xlabel("year", fontsize=16);
# plt.ylabel('prescription rate(%)', fontsize=15);
# plt.title('prescription rate', fontsize=15)
# plt.tick_params(axis='both', which='major', labelsize=15)



df_list = []
df = med_dict['total']
exposure_list = []
population_list = []
prevalence_dict = {}
med_df = df.iloc[:,72:1516]
age_group_list = []
for age_group in [ '17 or younger',  '18-24', '25-29', '30-34','35-39', '40 or older']:
  age_df = df[df['age_group'] == age_group]
  med_df = age_df.iloc[:,72:1516]
  exposure_list.append(sum(med_df.max(axis=1))) 
  age_group_list.append(age_group)
  population_list.append(len(age_df))
prevalence_dict['age_group'] = age_group_list 
prevalence_dict['exposure'] = exposure_list
prevalence_dict['population'] = population_list
prevalence_pd = pd.DataFrame.from_dict(prevalence_dict)
prevalence_pd['prevalence rate(%)'] = round(prevalence_pd['exposure']/prevalence_pd['population']*100,1)




# plt.ylim([0, 100])
# sns.set()
# sns.set_theme(style="white", font_scale=1.3)
# ax= sns.barplot(x="age_group", y = 'prevalence rate(%)', data = prevalence_pd,  hue_order= [ '17 or younger',  '18-24', '25-29', '30-34','35-39', '40 or older'])
# ax.set_xticklabels([ '17 or younger',  '18-24', '25-29', '30-34','35-39', '40 or older'])
# ax.bar_label(ax.containers[0])



df_list = []
df = med_dict['ip']
exposure_list = []
population_list = []
prevalence_dict = {}
med_df = df.iloc[:,72:1516]
age_group_list = []
for age_group in [ '17 or younger',  '18-24', '25-29', '30-34','35-39', '40 or older']:
  age_df = df[df['age_group'] == age_group]
  med_df = age_df.iloc[:,72:1516]
  exposure_list.append(sum(med_df.max(axis=1))) 
  age_group_list.append(age_group)
  population_list.append(len(age_df))
prevalence_dict['age_group'] = age_group_list 
prevalence_dict['exposure'] = exposure_list
prevalence_dict['population'] = population_list
prevalence_pd = pd.DataFrame.from_dict(prevalence_dict)
prevalence_pd['prevalence rate(%)'] = round(prevalence_pd['exposure']/prevalence_pd['population']*100,1)



df_list = []
df = med_dict['total']
exposure_list = []
population_list = []
prevalence_dict = {}
insurance_group_list = []
for insurance_group in [ 'Medicaid_Medicare', 'Commercial']:
  insurance_df = df[(df['age_group'] == age_group)]
  med_df = insurance_df.iloc[:,72:1516]
  exposure_list.append(sum(med_df.max(axis=1))) 
  insurance_group_list.append(insurance_group)
  population_list.append(len(insurance_df ))
prevalence_dict['insurance_group'] = insurance_group_list
prevalence_dict['exposure'] = exposure_list
prevalence_dict['population'] = population_list
prevalence_pd = pd.DataFrame.from_dict(prevalence_dict)
prevalence_pd['prevalence rate(%)'] = round(prevalence_pd['exposure']/prevalence_pd['population']*100,1)



df_list = []
df = med_dict['total']
exposure_list = []
population_list = []
prevalence_dict = {}
race_list = []
for race in [ 'Asian', 'American Indian or Alaska Native', 'Black or African American', 'Native Hawaiian or Other Pacific Islander', 'White or Caucasian', 'Multirace', 'Other']:
  race_df = df[df['race_group'] == race]
  med_df = race_df.iloc[:,72:1516]
  exposure_list.append(sum(med_df.max(axis=1))) 
  race_list.append(race)
  population_list.append(len(race_df))
prevalence_dict['race'] = race_list 
prevalence_dict['exposure'] = exposure_list
prevalence_dict['population'] = population_list
prevalence_pd = pd.DataFrame.from_dict(prevalence_dict)
prevalence_pd['prevalence rate(%)'] = round(prevalence_pd['exposure']/prevalence_pd['population']*100,1)

# comorbities 

med_df = spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_meds_pivoted_total_med2")
def get_prepregnancy_comorbidity (cohort_df, time_filter_string = None, rename_count = 'count_prepregnancy_problems', rename_list = 'list_prepregnancy_problems'):
  problem_list_df = get_problem_list(cohort_df = cohort_df,
                                     include_cohort_columns = ['pat_id', 'instance', 'ob_delivery_delivery_date','lmp']).filter(F.col('noted_date')<F.col('lmp')).filter(((F.col('resolved_date')>F.col('lmp'))|(F.col('resolved_date').isNull()))).filter((F.col('problem_status')!='Deleted'))
  if time_filter_string:
    problem_list_df = problem_list_df.filter(time_filter_string)
  get_count = F.udf(lambda s: len(s), IntegerType())
  problemlist_ag = aggregate_data(problem_list_df, partition_columns = ['pat_id', 'instance', 'ob_delivery_delivery_date','lmp'], aggregation_columns = {'dx_id':'collect_set', 'name':'collect_set'}).withColumn(rename_count, get_count(F.col('dx_id_collect_set'))).withColumnRenamed('name_collect_set', rename_list).drop('dx_id_collect_set') 
  
  return_df = cohort_df.join(problemlist_ag, ['pat_id', 'instance', 'ob_delivery_delivery_date','lmp'], 'left').fillna(0, subset = [rename_count])
  return return_df 
def get_prenatal_comorbidity (cohort_df, time_filter_string = None, rename_count = 'count_prenatal_problems', rename_list = 'list_prenatal_problems'):
  problem_list_df = get_problem_list(cohort_df = cohort_df,
                                     include_cohort_columns = ['pat_id', 'instance', 'ob_delivery_delivery_date','lmp']).filter((F.col('noted_date')>=F.col('lmp'))&(F.col('noted_date')<F.col('ob_delivery_delivery_date'))).filter(((F.col('resolved_date')>F.col('lmp'))|(F.col('resolved_date').isNull()))).filter((F.col('problem_status')!='Deleted'))
  if time_filter_string:
    problem_list_df = problem_list_df.filter(time_filter_string)
  get_count = F.udf(lambda s: len(s), IntegerType())
  problemlist_ag = aggregate_data(problem_list_df, partition_columns = ['pat_id', 'instance', 'ob_delivery_delivery_date','lmp'], aggregation_columns = {'dx_id':'collect_set', 'name':'collect_set'}).withColumn(rename_count, get_count(F.col('dx_id_collect_set'))).withColumnRenamed('name_collect_set', rename_list).drop('dx_id_collect_set') 
  
  return_df = cohort_df.join(problemlist_ag, ['pat_id', 'instance', 'ob_delivery_delivery_date','lmp'], 'left').fillna(0, subset = [rename_count])
  return return_df 
prepregnancy_comorbidity = get_prepregnancy_comorbidity(med_df)
total_comorbidity = get_prenatal_comorbidity(prepregnancy_comorbidity)



risk_pd = total_comorbidity.toPandas()



def get_risk_group(risk):
    if risk == 0:
      return '0'
    elif risk > 0 and risk < 6:
      return '1-5'
    elif risk >= 6 and risk < 11:
      return '6-10'
    else:
      return '11+'
    
risk_pd['prepregnancy_comorbidity_count_category'] = risk_pd['count_prepregnancy_problems'].apply(get_risk_group)
risk_pd['prenatal_comorbidity_count_category'] = risk_pd['count_prenatal_problems'].apply(get_risk_group)

# COMMAND ----------

risk_pd['prepregnancy_comorbidity_count_category'].value_counts()

# COMMAND ----------

risk_pd['prenatal_comorbidity_count_category'].value_counts()

# COMMAND ----------

df_list = []
df = risk_pd
exposure_list = []
population_list = []
prevalence_dict = {}
risk_group_list = []
comorbidity_list = []
for c in ['prepregnancy_comorbidity_count_category', 'prenatal_comorbidity_count_category']:
  for risk_group in [ '0', '1-5', '6-10', '11+']:
    risk_df = df[(df[c] == risk_group)]
    comorbidity_list.append(c)
    med_df = risk_df.iloc[:,72:1516]
    exposure_list.append(sum(med_df.max(axis=1))) 
    risk_group_list.append(risk_group)
    population_list.append(len(risk_df ))
prevalence_dict['comorbidity_type'] = comorbidity_list
prevalence_dict['risk_group'] = risk_group_list
prevalence_dict['exposure'] = exposure_list
prevalence_dict['population'] = population_list
prevalence_pd = pd.DataFrame.from_dict(prevalence_dict)
prevalence_pd['prevalence rate(%)'] = round(prevalence_pd['exposure']/prevalence_pd['population']*100,1)
prevalence_pd

# COMMAND ----------

plt.ylim([0, 100])
sns.set()
sns.set_theme(style="white", font_scale=1)
ax= sns.barplot(x="risk_group", y = 'prevalence rate(%)', hue = 'comorbidity_type', data = prevalence_pd)
ax.set_xticklabels(['0', '1-5', '6-10', '11+'])
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.,  frameon=False)

# COMMAND ----------

risk_pd = risk_pd[risk_pd.delivery_year>2012] 
sns.set_theme(style="white", font_scale=1.5)
g =sns.histplot(x="delivery_year", hue = 'prepregnancy_comorbidity_count_category', data = risk_pd, stat="percent", multiple="fill",  hue_order=['0', '1-5', '6-10', '11+'], discrete = True, element = 'bars')
sns.move_legend(g, "upper left", bbox_to_anchor=(1, 1))
g.set(xlabel = 'delivery year', ylabel = 'percent(%)')   

# COMMAND ----------



# COMMAND ----------

risk_pd = risk_pd[risk_pd.delivery_year>2012] 
sns.set_theme(style="white", font_scale=1.5)
g =sns.histplot(x="delivery_year", hue = 'prenatal_comorbidity_count_category', data = risk_pd, stat="percent", multiple="fill",  hue_order=['0', '1-5', '6-10', '11+'], discrete = True, element = 'bars')
sns.move_legend(g, "upper left", bbox_to_anchor=(1, 1))
g.set(xlabel = 'delivery year', ylabel = 'percent(%)')   

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

med_sum_row = med_dict['total'].iloc[:,72:1516].sum(axis=0) 
op_sum_row = med_dict['op'].iloc[:,72:1516].sum(axis=0) 
ip_sum_row = med_dict['ip'].iloc[:,72:1516].sum(axis=0) 

# COMMAND ----------

top_10 = list(med_sum_row.to_frame(name = "count").sort_values(by = "count", ascending = False)[0:10].index)

# COMMAND ----------

op_top_10 = list(op_sum_row.to_frame(name = "count").sort_values(by = "count", ascending = False)[0:10].index)
op_top_10

# COMMAND ----------

len(op_sum_row.to_frame(name = "count").sort_values(by = "count", ascending = False))

# COMMAND ----------

med_sum_row.to_frame(name = "count").loc['sertraline']['count']

# COMMAND ----------

med_list = []
exposure_list = []
population_list = []
year_list = []
prevalence_dict = {}
df = med_dict['total']
for year in [2013,2014,2015,2016,2017,2018,2019,2020,2021,2022]:
  year_df = df[df['delivery_year'] == year]
  med_df = year_df.iloc[:,72:1516]
  med_sum_df = med_df.iloc[:,72:1516].sum(axis=0).to_frame(name = "count") 
  for med in top_10:
    med_count = med_sum_df.loc[med]['count']
    exposure_list.append(med_count) 
    year_list.append(year)
    med_list.append(med)
    population_list.append(len(year_df))
prevalence_dict['year'] = year_list 
prevalence_dict['med'] = med_list 
prevalence_dict['exposure'] = exposure_list
prevalence_dict['population'] = population_list
prevalence_pd = pd.DataFrame.from_dict(prevalence_dict)

# COMMAND ----------

prevalence_pd['prevalence rate(%)'] = prevalence_pd['exposure']/prevalence_pd['population']*100
prevalence_pd

# COMMAND ----------

sns.set_theme(style="white", font = "Arial", font_scale=1.5)
plt.ylim([0, 30])
sns.lineplot(
  data=prevalence_pd,
  x="year", y="prevalence rate(%)", hue="med", style="med", 
  markers=True, dashes=False, palette = 'colorblind'
)
plt.xticks([2013,2014,2015,2016,2017,2018,2019,2020,2021,2022])
plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.,  frameon=False)
plt.xlabel("year", fontsize=16);
plt.ylabel('prescription rate(%)', fontsize=15);
plt.title('prescription rate', fontsize=15)
plt.tick_params(axis='both', which='major', labelsize=15)

# COMMAND ----------

ATC_df_dict = {'ATC_total' : spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_ATC_med_status").withColumn('delivery_year', F.year('ob_delivery_delivery_date')).filter(F.col('delivery_year')>2012).filter(F.col('delivery_year')<2023).toPandas(),
'ATC_op' : spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_ATC_med_op_status").withColumn('delivery_year', F.year('ob_delivery_delivery_date')).filter(F.col('delivery_year')>2012).filter(F.col('delivery_year')<2023).toPandas(),
'ATC_ip' : spark.sql("SELECT * FROM rdp_phi_sandbox.yh_maternity_ATC_med_ip_status").withColumn('delivery_year', F.year('ob_delivery_delivery_date')).filter(F.col('delivery_year')>2012).filter(F.col('delivery_year')<2023).toPandas()
}

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.font_manager
import seaborn as sns
sns.set_theme(style="white", font = "Arial", font_scale=1.5)

# COMMAND ----------

mapping_df = spark.sql("SELECT * FROM rdp_phi_sandbox.hadlock_medication_id_atc_mapping")

# COMMAND ----------

first = []
second = []
third = []
fourth = []
fifth = []
for i in mapping_df.columns:
  if len(i) == 1:
    first.append(i)
  elif len(i) == 3:
    second.append(i)
  elif len(i) == 4:
    third.append(i)
  elif len(i) == 5:
    fourth.append(i)
  elif len(i) == 7:
    fifth.append(i)
ATC_list = first + second + third + fourth + fifth 

# COMMAND ----------

first_total = ATC_df_dict['ATC_total'][first].sum(axis = 0).to_frame(name = 'count')
first_op = ATC_df_dict['ATC_op'][first].sum(axis = 0).to_frame(name = 'count')
first_ip = ATC_df_dict['ATC_ip'][first].sum(axis = 0).to_frame(name = 'count')

# COMMAND ----------

first_total = first_total.reset_index()
first_op = first_op.reset_index()
first_ip = first_ip.reset_index()

# COMMAND ----------

first_total['prevalence'] = first_total['count']/365074*100
first_op['prevalence'] = first_op['count']/365074*100
first_ip['prevalence'] = first_ip['count']/365074*100

# COMMAND ----------

first_total['type'] = 'total'
first_op['type'] = 'op'
first_ip['type'] = 'ip'

# COMMAND ----------

import pandas as pd
first = pd.concat([first_total, first_op, first_ip])

# COMMAND ----------

first

# COMMAND ----------

import seaborn as sns
sns.set(font_scale=2)
sns.set(style = "whitegrid")
sns.set(font="Arial")
sns.set(rc={'figure.figsize':(10,10)})
fig = sns.barplot(
    data=first, x = 'prevalence', y = 'index', hue = 'type', capsize=.4, errcolor=".5")
fig.set_xlim(0, 100)

# COMMAND ----------

fourth_total = ATC_df_dict['ATC_total'][fourth].sum(axis = 0).to_frame(name = 'count')
fourth_op = ATC_df_dict['ATC_op'][fourth].sum(axis = 0).to_frame(name = 'count')
fourth_ip = ATC_df_dict['ATC_ip'][fourth].sum(axis = 0).to_frame(name = 'count')

# COMMAND ----------

fourth_total = fourth_total.reset_index()
fourth_op = fourth_op.reset_index()
fourth_ip = fourth_ip.reset_index()

# COMMAND ----------

fourth_total['prevalence'] = fourth_total['count']/365074*100
fourth_op['prevalence'] = fourth_op['count']/365074*100
fourth_ip['prevalence'] = fourth_ip['count']/365074*100

# COMMAND ----------

fourth_total['type'] = 'total'
fourth_op['type'] = 'op'
fourth_ip['type'] = 'ip'

# COMMAND ----------

import pandas as pd
fourth = pd.concat([fourth_total, fourth_op, fourth_ip])

# COMMAND ----------

top10_med = list(fourth_total.sort_values(by = 'count', ascending=False).head(10)['index'])

# COMMAND ----------

top_10_df = fourth[fourth['index'].isin(top10_med)]
top_10_df

# COMMAND ----------

sns.set(rc={'figure.figsize':(10,10)})
fig = sns.barplot(
    data=top_10_df, x = 'index', y = 'prevalence', hue = 'type', capsize=.4, errcolor=".5", order = top10_med)
fig.set_ylim(0, 100)

# COMMAND ----------

first_total.head(10)

# COMMAND ----------

med_list = []
exposure_list = []
population_list = []
year_list = []
prevalence_dict = {}
df = ATC_df_dict['ATC_total']
for year in [2013,2014,2015,2016,2017,2018,2019,2020,2021,2022]:
  year_df = df[df['delivery_year'] == year]
  med_df = ATC_df_dict['ATC_total'][first]
  med_sum_df = ATC_df_dict['ATC_total'][first].sum(axis=0).to_frame(name = "count") 
  for med in first:
    med_count = med_sum_df.loc[med]['count']
    exposure_list.append(med_count) 
    year_list.append(year)
    med_list.append(med)
    population_list.append(len(year_df))
prevalence_dict['year'] = year_list 
prevalence_dict['med'] = med_list 
prevalence_dict['exposure'] = exposure_list
prevalence_dict['population'] = population_list
prevalence_pd = pd.DataFrame.from_dict(prevalence_dict)