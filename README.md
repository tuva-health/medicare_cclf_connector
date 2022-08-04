[![Apache License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) ![dbt logo and version](https://img.shields.io/static/v1?logo=dbt&label=dbt-version&message=1.x&color=orange)

# Medicare CCLF Connector

Check out the [Tuva Data Models](https://docs.google.com/spreadsheets/d/1NuMEhcx6D6MSyZEQ6yk0LWU0HLvaeVma8S-5zhOnbcE/edit?usp=sharing)

Check out our [Docs](http://thetuvaproject.com/)

This connector models Medicare CCLF into the Tuva Claims Input Layer which enables you to run most of the other components of the Tuva Project with very little effort.

## Pre-requisites
1. You have Medicare CCLF claims data loaded into a data warehouse
2. You have [dbt](https://www.getdbt.com/) installed and configured (i.e. connected to your data warehouse)

[Here](https://docs.getdbt.com/dbt-cli/installation) are instructions for installing dbt.

## Getting Started
Complete the following steps to configure the package to run in your environment.

1. [Clone](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository) this repo to your local machine or environment
2. Run the following command, filling in your input (i.e. where your source data is located) and output (i.e. where you want the data processed by this module to be written to) data locations: dbt build --vars '{key: value, input_database: syntegra_synthetic_sample, input_schema: cclf, output_database: demo, output_schema: claims_input_layer}'

Note: The source data table names need to match the table names in [sources.yml](models/sources.yml).  These table names match the [Medicare CCLF data dictionary](https://www.cms.gov/files/document/cclf-file-data-elements-resource.pdf).  If you rename any tables make sure you:
    - Update table names in sources.yml
    - Update table name in medical_claim and eligibility jinja function

## Contributions
Have an opinion on the mappings? Notice any bugs when installing and running the package? 
If so, we highly encourage and welcome contributions! 

Join the conversation on [Slack](https://tuvahealth.slack.com/ssb/redirect#/shared-invite/email)!  We'd love to hear from you on the #claims-preprocessing channel.

## Database Support
This package has been built and tested on:
    - Snowflake
    - Redshift
