# PSM_Maternity_at_scale

This contains the code used to conduct the retrospective cohort study for Hwang et al. 2024 manuscript on <b>Accelerating Adverse Pregnancy Outcomes Research Amidst Rising Medication Use: Parallel Retrospective Cohort Analyses for Signal Prioritization</b>.
Preprint can be found in [medrxiv](https://www.medrxiv.org/content/10.1101/2024.03.21.24304579v1)

All clinical logic is shared. Results were aggregated and reported within the paper to the extent possible while maintaining privacy from personal health information (PHI) as required by law. All data is archived within PHS systems in a HIPAA-secure audited compute environment to facilitate verification of study conclusions. Due to the sensitive nature of the data we are unable to share the data used in these analyses, although, all scripts used to conduct the analyses of the paper are shared herein.

For codes of ETL tools, please refer to https://github.com/Hadlock-Lab/CEDA_tools_ver20221220

Installation
We used Python version 3.8.10.

Workflow
Our workflow is described using alphabets.

[utilities](https://github.com/Hadlock-Lab/COVID19_Maternity_Inpatient_Anticoagulant/tree/main/utilities) contains functions written by Hadlock Lab and required to be loaded for analysis. This include codes for maternity cohort generation. 

[A_save_medication](https://github.com/Hadlock-Lab/PSM_Maternity_at_scale/tree/main/A_save_medication) saves medication records and generate ATC classification table.

[B_comorbidities](https://github.com/Hadlock-Lab/PSM_Maternity_at_scale/tree/main/B_comorbidities) saves comorbidity record and prepare comorbidity dataframe.

[C_descriptive_statistics](https://github.com/Hadlock-Lab/PSM_Maternity_at_scale/tree/main/C_descriptive_statistics) generate table one and descriptive figures on medication records 

[D_analysis](https://github.com/Hadlock-Lab/PSM_Maternity_at_scale/tree/main/D_analysis) prepares cohort to be run on propensity score matching at scale

[E_additional](https://github.com/Hadlock-Lab/PSM_Maternity_at_scale/tree/main/E_additional) includes additionaly analysis codes on sertraline, ferrous sulfate, and acyclovir. 
For further investigation on sertraline, please refer to [Hwang et al. 2023](https://www.tandfonline.com/doi/full/10.1080/14767058.2024.2313364#d1e451) and [github link](https://github.com/Hadlock-Lab/YH_SSRI_PTB) 
