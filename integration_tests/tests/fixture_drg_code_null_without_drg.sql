{{ config(tags=['fixture']) }}

/*
    CCLF1 DGNS_DRG_CD is a four-character code. The fixture claims without a
    DRG (outpatient, SNF, home health and hospice) carry the zero-filled
    '0000'; the inpatient claims carry a real MS-DRG ('0291').

    '000' is not an MS-DRG, so no medical_claim row may have drg_code '000'
    (or an empty or space-only drg_code), and the inpatient claims must keep
    their DRG without the leading zero.
*/

select
      claim_id
    , claim_line_number
    , claim_type_code
    , drg_code
from {{ ref('medical_claim') }}
where trim(drg_code) in ('', '000', '0000')
   or (claim_type_code = '60' and drg_code is distinct from '291')
