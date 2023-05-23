[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.x&color=orange)

# Medicare CCLF Connector

## 🔗  Quick Links
- [Docs](https://tuva-health.github.io/the_tuva_project/#!/overview): Learn about the Tuva Project data model
- [Knowledge Base](https://thetuvaproject.com/docs/intro): Learn about claims data fundamentals and how to do claims data analytics
<br/><br/>

## 🧰  What does this project do?

This connector is a dbt project that transforms raw Medicare CCLF claims data through the entire Tuva Project, including the Tuva Claims Data Model and all the data marts.  This connector expects your CCLF data to be organized into the tables outlined in this [CMS data dictionary](https://www.cms.gov/files/document/cclf-information-packet.pdf), which is the most recent format CMS uses to distribute CCLF files.
<br/><br/>  

## 🔌 Database Support

- BigQuery
- Redshift
- Snowflake
<br/><br/>  

## ✅  Quick Start Guide

### Step 1: Fork Connector and Import Tuva Project
Unlike [the Tuva Project](https://github.com/tuva-health/the_tuva_project), this repo is a dbt project, not a dbt package.  Clone or fork this repository.  Then `cd` into the directly where you cloned this project into and run `dbt deps` to import the latest version of the Tuva Project.
<br/><br/> 

### Step 2: Configure Variables in `dbt_project.yml`

The yml for these variables already ships with the project, you just need to edit it.  Configure the following variables in the `dbt_project.yml` file to tell dbt where the source cclf data is located in your data warehouse:
1. `input_database`: The name of database where your raw cclf data is located.
2. `input_schema`: The name of the schema where your raw cclf data is located.
3. Input Data Tables: The project expects your cclf tables to have specific names.  
```yaml
#########################################
#### Medicare CCLF Project Variables ####
#########################################

  data_source: cclf             

## Input Data Configurations:
## Set the input_database variable and input_schema variable to the database
## and schema where the raw cclf data is stored.
  input_database: source_data  
  input_schema: cclf                          

## This project expects 7 standard cclf input tables with the following names.
## If your tables have different names, adjust the values of the variables 
## below accordingly.
  beneficiary_demographics_table: beneficiary_demographics
  parta_claims_header_table: parta_claims_header
  parta_claims_revenue_center_detail_table: parta_claims_revenue_center_detail
  parta_diagnosis_code_table: parta_diagnosis_code
  parta_procedure_code_table: parta_procedure_code
  partb_physicians_table: partb_physicians
  partb_dme_table: partb_dme               

## Set the medicare_cclf_connector_schema variable to tell dbt where you want to write 
## the cclf data that has been transformed into the Tuva Claims Data Model format.
  medicare_cclf_connector_schema: claims_common 
```

Next, configure the Tuva Project variables.  You can see these variables in the yaml below.

1. Package Enabled Variables: Set to true or false depending on which packages you want to run or not run.
2. `tuva_database`: Sets the target database i.e. where data will be written to
```yaml
########################################
#### Tuva Project Package Variables ####
########################################

## Package Enabled Variables:
## These variables tell the Tuva Project which packages you want
## to enable.  To enable a package set it to true, to disable a 
## package set it to false.
  claims_preprocessing_enabled: true
  cms_chronic_conditions_enabled: true
  data_profiling_enabled: true 
  pmpm_enabled: true
  readmissions_enabled: true
  terminology_enabled: true
  tuva_chronic_conditions_enabled: true


## Target Database Variable:
## This variable tells the Tuva Project where to write the 
## output data to.  You must create this database in your
## data warehouse before running the Tuva Project.
  tuva_database: tuva  
```
### Step 3: Run Project
`cd` to the project root folder in the command line and execute `dbt build`.  Next you're now ready to do claims data analytics!
<br/><br/>

## 🙋🏻‍♀️ How is this package maintained and how do I contribute?

### Project Maintenance

The Tuva Project team maintaining this project **only** maintains the latest version of the project. 
We highly recommend you stay consistent with the latest version.

### Contributions

Have an opinion on the mappings? Notice any bugs when installing and running the project?
If so, we highly encourage and welcome feedback!  While we work on a formal process in Github, we can be easily reached on our Slack community.
<br/><br/>

## 🤝 Join our community!

Join our growing community of healthcare data practitioners on [Slack](https://join.slack.com/t/thetuvaproject/shared_invite/zt-16iz61187-G522Mc2WGA2mHF57e0il0Q)!
