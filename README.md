[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.x&color=orange)

# Medicare CCLF Connector

## 🔗 Docs
Check out our [docs](https://thetuvaproject.com/) to learn about the project and how you can use it.
<br/><br/>

## 🧰 What does this repo do?

The Medicare CCLF Connector is a dbt project that maps raw Medicare CCLF claims data to the Tuva Input Layer, which is the first step in running the Tuva Project.  This connector expects your CCLF data to be organized into the tables outlined in this [CMS data dictionary](https://www.cms.gov/files/document/cclf-information-packet.pdf), which is the most recent format CMS uses to distribute CCLF files.
<br/><br/>  

## 🔌 Database Support

- BigQuery
- Redshift
- Snowflake
<br/><br/>  

## ✅ Quickstart Guide

### Step 1: Clone or Fork this Repository
Unlike [the Tuva Project](https://github.com/tuva-health/the_tuva_project), this repo is a dbt project, not a dbt package.  Clone or fork this repository to your local machine.
<br/><br/> 

### Step 2: Import the Tuva Project
Next you need to import the Tuva Project dbt package into the Medicare CCLF Connector dbt project.  For example, using dbt CLI you would `cd` into the directly where you cloned this project to and run `dbt deps` to import the latest version of the Tuva Project.
<br/><br/> 

### Step 3: Data Preparation

#### Source data:
The source table names the connector is expecting can be found in the 
`_sources.yml` config file. You can rename your source tables if needed or add an 
alias to the config.  

#### File Dates:
The field `file_date` is used throughout this connector to deduplicate data 
received across regular and run-out CCLFs. We recommend parsing this date from 
the filename (e.g., P.A****.ACO.ZC1Y**.Dyymmdd.Thhmmsst) and formatting it as 
"YYYY-MM-DD".

#### Enrollment Dates:
The CCLF specification does not have a field that can be mapped directly 
to `enrollment_start_date` and `enrollment_end_date`, and the Part A and Part B 
entitlement dates (BENE_PART_A_ENRLMT_BGN_DT, BENE_PART_B_ENRLMT_BGN_DT) are 
often incorrect or not useful for claims analytics.

We have included an additional source called `Enrollment` that can be
populated with enrollment dates relevant to your data. These enrollment
dates may come from an attribution file, beneficiary alignment report (BAR), or
any source you may have. You just need to create a source table with the 
following columns:

  1. `current_bene_mbi_id`
  2. `enrollment_start_date`
  3. `enrollment_end_date`
  4. `bene_member_month`
     * The connector includes logic to handle enrollment spans or member months.
     * If enrollment spans are available, leave this field null.
     * If enrollment spans are not available, populate this field with member 
       month dates in the format "YYYY-MM-DD" and set the variable 
       `member_months_enrollment` to true in the `dbt_project.yml` file.
<br/><br/> 

### Step 4: Configure Input Database and Schema
Next you need to tell dbt where your Medicare CCLF source data is located.  Do this using the variables `input_database` and `input_schema` in the `dbt_project.yml` file.  You also need to configure your `profile` in the `dbt_project.yml`.
<br/><br/> 

### Step 5: Run
Finally, run the connector and the Tuva Project. For example, using dbt CLI you would `cd` to the project root folder in the command line and execute `dbt build`.  

Now you're ready to do claims data analytics!
<br/><br/>

## 🙋🏻‍♀️ How do I contribute?
Have an opinion on the mappings? Notice any bugs when installing and running the project?
If so, we highly encourage and welcome feedback!  While we work on a formal process in Github, we can be easily reached on our Slack community.
<br/><br/>

## 🤝 Join our community!
Join our growing community of healthcare data practitioners on [Slack](https://join.slack.com/t/thetuvaproject/shared_invite/zt-16iz61187-G522Mc2WGA2mHF57e0il0Q)!
