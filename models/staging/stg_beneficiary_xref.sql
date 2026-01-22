with beneficiary_xref as (
  SELECT * FROM
  {% if var('demo_data_only', false) %} {{ ref('beneficiary_xref') }} {% else %} {{ source('medicare_cclf','beneficiary_xref') }}{% endif %}
)

select
      HICN_MBI_XREF_IND
    , CRNT_NUM
    , PRVS_NUM
    , PRVS_ID_EFCTV_DT
    , PRVS_ID_OBSLT_DT
    , BENE_RRB_NUM
    , FILE_NAME
    , FILE_DATE
from beneficiary_xref