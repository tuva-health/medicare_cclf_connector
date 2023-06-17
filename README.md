[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.x&color=orange)

# Medicare CCLF Connector

## 🔗  Docs
Check out our [docs](https://thetuvaproject.com/) to learn about the project and how you can use it.
<br/><br/>

## 🧰  What does this repo do?

The Medicare CCLF Connector is a dbt project that maps raw Medicare CCLF claims data to the Tuva claims data model and then builds all of the Tuva data marts.  This connector expects your CCLF data to be organized into the tables outlined in this [CMS data dictionary](https://www.cms.gov/files/document/cclf-information-packet.pdf), which is the most recent format CMS uses to distribute CCLF files.
<br/><br/>  

## 🔌 Database Support

- BigQuery
- Redshift
- Snowflake
<br/><br/>  

## ✅  Quickstart Guide

### Step 1: Clone or Fork this Repository
Unlike [the Tuva Project](https://github.com/tuva-health/the_tuva_project), this repo is a dbt project, not a dbt package.  Clone or fork this repository to your local machine.
<br/><br/> 

### Step 2: Import the Tuva Project
Next you need to import the Tuva Project dbt package into the Medicare CCLF Connector dbt project.  For example, using dbt CLI you would `cd` into the directly where you cloned this project to and run `dbt deps` to import the latest version of the Tuva Project.
<br/><br/> 

### Step 3: Configure Input Database and Schema
Next you need to tell dbt where your Medicare CCLF source data is located.  Do this using the variables `input_database` and `input_schema` in the `dbt_project.yml` file.  You also need to configure your `profile` in the `dbt_project.yml`.
<br/><br/> 

### Step 4: Run
Now you're ready to run the connector and the Tuva Project.  For example, using dbt CLI you would `cd` to the project root folder in the command line and execute `dbt build`.  Next you're now ready to do claims data analytics!  Check out the [data mart](https://thetuvaproject.com/data-marts/about) in our docs to learn what tables you should query.
<br/><br/>

## 🙋🏻‍♀️ How do I contribute?
Have an opinion on the mappings? Notice any bugs when installing and running the project?
If so, we highly encourage and welcome feedback!  While we work on a formal process in Github, we can be easily reached on our Slack community.
<br/><br/>

## 🤝 Join our community!
Join our growing community of healthcare data practitioners on [Slack](https://join.slack.com/t/thetuvaproject/shared_invite/zt-16iz61187-G522Mc2WGA2mHF57e0il0Q)!
