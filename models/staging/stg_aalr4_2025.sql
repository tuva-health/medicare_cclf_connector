SELECT
  BENE_MBI_ID,
  MASTER_ID,
  NPI_USED,
  PCS_COUNT
from {{ source('medicare_cclf','aalr4_2025') }} as a