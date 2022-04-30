select
     encounter_id
    , patient_id
    , procedure_date
    , code_type
    , code
    , description
    , physician_npi
    , data_source
from {{ ref('procedure_dme')}}

union all 

select
     encounter_id
    , patient_id
    , procedure_date
    , code_type
    , code
    , description
    , physician_npi
    , data_source
from {{ ref('procedure_professional')}}

union all 

select
     encounter_id
    , patient_id
    , procedure_date
    , code_type
    , code
    , description
    , physician_npi
    , data_source
from {{ ref('procedure_institutional')}}