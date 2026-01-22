with enrollment as (
  SELECT * FROM
  {% if var('demo_data_only', false) %} {{ ref('enrollment') }} {% else %} {{ source('medicare_cclf','enrollment') }}{% endif %}
)

select
      CURRENT_BENE_MBI_ID
    , ENROLLMENT_START_DATE
    , ENROLLMENT_END_DATE
    , BENE_MEMBER_MONTH
    , FILE_NAME
    , FILE_DATE
from enrollment