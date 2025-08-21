select
      current_bene_mbi_id
    , enrollment_start_date
    , enrollment_end_date
    , bene_member_month
from {{ source('medicare_cclf','enrollment') }}