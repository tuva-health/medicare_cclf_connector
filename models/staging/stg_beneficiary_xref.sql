-- CTE that selects from either the source table or the demo data seed based on the 'demo_data_only' variable
with beneficiary_xref as (
  SELECT 
    *
  FROM
  {% if var('demo_data_only', false) %} {{ ref('beneficiary_xref') }} {% else %} {{ source('medicare_cclf','cclf9_claim') }}{% endif %}
)

select
      HICN_MBI_XREF_IND
    , CRNT_NUM
    , PRVS_NUM
    , {{ try_to_cast_date('PRVS_ID_EFCTV_DT') }} as PRVS_ID_EFCTV_DT
    , {{ try_to_cast_date('PRVS_ID_OBSLT_DT') }} as PRVS_ID_OBSLT_DT
    , BENE_RRB_NUM
    , FILE_GROUP_ID as FILE_NAME
    , null as FILE_DATE
from beneficiary_xref
