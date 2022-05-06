with stage as(
  select
     cast(encounter_id as varchar) as encounter_id
    , cast(patient_id as varchar) as patient_id
    , cast(condition_date as date) as condition_date
    , cast(condition_type as varchar) as condition_type
    , cast(code_type as varchar) as code_type
    , cast(code as varchar) as code
    , cast(description as varchar) as description
    , cast(diagnosis_rank as int) as diagnosis_rank
    , cast(present_on_admit as varchar) as present_on_admit
    , cast(data_source as varchar) as data_source
  from {{ ref('condition_institutional')}}
  
  union all
  
  select
     cast(encounter_id as varchar) as encounter_id
    , cast(patient_id as varchar) as patient_id
    , cast(condition_date as date) as condition_date
    , cast(condition_type as varchar) as condition_type
    , cast(code_type as varchar) as code_type
    , cast(code as varchar) as code
    , cast(description as varchar) as description
    , cast(diagnosis_rank as int) as diagnosis_rank
    , cast(present_on_admit as varchar) as present_on_admit
    , cast(data_source as varchar) as data_source
  from {{ ref('condition_professional')}}
)
, duplicates as (
  select 
    *
    , row_number() over(partition by encounter_id, condition_date, code, diagnosis_rank) as row_number 
  from stage 
)

select
   cast(encounter_id as varchar) as encounter_id
  , cast(patient_id as varchar) as patient_id
  , cast(condition_date as date) as condition_date
  , cast(condition_type as varchar) as condition_type
  , cast(code_type as varchar) as code_type
  , cast(code as varchar) as code
  , cast(description as varchar) as description
  , cast(diagnosis_rank as int) as diagnosis_rank
  , cast(present_on_admit as varchar) as present_on_admit
  , cast(data_source as varchar) as data_source
from duplicates
where row_number = 1