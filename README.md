# Claims DBT Sandbox

Based off the Tuva Medicare CCLF Connector, see their readme in README.tuva.md

## Set up

Install dbt 1.2+

Run the following
- `dbt deps`
- `dbt init` and enter your manganese snowflake creds

## Seed
Claims data is seeded from sample CCLF data available on [syntegra](https://www.syntegra.io/download-syntegra-data)
Place these files in the seeds/ folder

If the database is empty then you need to run:
- `dbt seed` - one time to seed the initial tables

## Run
- `dbt build --select path:./models`
- `dbt build --select cms_hcc`
