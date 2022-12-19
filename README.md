[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.x&color=orange)

# Medicare CCLF Claims Connector

## 🧰 What does the Medicare CCLF Claims Connector do?

This connector transforms raw Medicare CCLF claims data into the Tuva Claims Input Layer which enables you to run the other components of the Tuva Project.
For a detailed overview of what the project does and how it works, check out our [Knowledge Base](https://thetuvaproject.com/docs/getting-started). 
For information on data models and to view the entire DAG check out our dbt [Docs](https://tuva-health.github.io/medicare_cclf_connector/#!/overview?g_v=1).

## 🧰 What does The Tuva Project do?

The Tuva Project is a collection of dbt packages that builds healthcare concepts (measures, groupers, data quality tests) on top of your raw healthcare claims data. Currently, the Tuva Project consists of the following 5 dbt packages, each of which is a separate GitHub repo that does something specific:

- `data_profiling`: Runs data quality tests to check for common problems specific to healthcare claims data.
- `claims_preprocessing`: Groups overlapping claims into a single encounter, assigns every claim to 1 of 18 different encounter types and populates core concept tables.
- `chronic_conditions`: Implements a chronic condition grouper based on ICD-10-CM codes. As a result, it is possible to know whether each patient in your population has any of ~70 different chronic conditions defined for the grouper.
- `readmissions`: Calculates hospital readmission measures.
- `terminology`: Makes the latest version of many useful healthcare terminology datasets available as tables in your data warehouse. This package is different from the others because it does not build healthcare concepts on top of your data.


## 🔌 Database Support

- Snowflake
- Redshift
- *BigQuery - connector only is supported; some marts in The Tuva Project may not run in BigQuery.


## ✅ How to get started

### Pre-requisites
1. You have Medicare CCLF claims data loaded into a data warehouse.
2. You have [dbt](https://www.getdbt.com/) installed and configured (i.e. connected to your data warehouse).
3. This project is dependent on the packages `dbt_utils`, include the following in your packages.yml:
   ```
   packages:
     - package: dbt-labs/dbt_utils
       version: [">=0.9.2","<1.0.0"]
   ```

[Here](https://docs.getdbt.com/dbt-cli/installation) are instructions for installing dbt.

### Getting Started
Complete the following steps to configure the project to run in your environment.

1. [Clone](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository) this repo to your local machine or environment
2. Update the dbt_project.yml file to use the dbt profile connected to your data warehouse.
   1. See dbt_project.yml file for other possible configurations defining input and output database and schema, as well as which marts are enabled.
3. Run dbt deps to import the dependent packages
4. Run dbt build to execute and build all models.
  

Note: The source data table names need to match the table names in [sources.yml](models/_sources.yml).  These table names match the [Medicare CCLF data dictionary](https://www.cms.gov/files/document/cclf-file-data-elements-resource.pdf).  
If you rename any tables make sure any changes are reflected in sources.yml.  If you rename any sources or if you have models creating the input tables, make sure the 
input variables in [dbt_project.yml](dbt_project.yml) are updated to reflect the new source or model names.


## 🙋🏻‍♀️ **How is this project maintained and can I contribute?**

### Project Maintenance

The Tuva Project team maintaining this project **only** maintains the latest version of the project. 
We highly recommend you stay consistent with the latest version.

### Contributions

Have an opinion on the mappings? Notice any bugs when installing and running the project?
If so, we highly encourage and welcome feedback!  While we work on a formal process in Github, we can be easily reached on our Slack community.

## 🤝 Community

Join our growing community of healthcare data practitioners on [Slack](https://join.slack.com/t/thetuvaproject/shared_invite/zt-16iz61187-G522Mc2WGA2mHF57e0il0Q)!
