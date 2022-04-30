{{ config(materialized='table') }}

select * from {{ ref('prof_encounter_crosswalk_hha')}}
union
select * from {{ ref('prof_encounter_crosswalk_hospice')}}
union 
select * from {{ ref('prof_encounter_crosswalk_inpatient')}}
union
select * from {{ ref('prof_encounter_crosswalk_snf')}}
