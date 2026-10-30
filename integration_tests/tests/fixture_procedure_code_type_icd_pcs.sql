{{ config(tags=['fixture']) }}

/*
    The inpatient fixture claims carry CCLF3 procedure codes (5A1D70Z, 30233N1)
    with DGNS_PRCDR_ICD_IND = '0'. Those are ICD-10-PCS codes, so every
    medical_claim row that has a procedure code must have procedure_code_type
    icd-10-pcs or icd-9-pcs, the values the Tuva input layer accepts for ICD
    procedure codes. icd-10-cm is the diagnosis code system.
*/

select
      claim_id
    , claim_line_number
    , procedure_code_type
    , procedure_code_1
from {{ ref('medical_claim') }}
where procedure_code_1 is not null
  and (
      procedure_code_type is null
      or procedure_code_type not in ('icd-10-pcs', 'icd-9-pcs')
  )
