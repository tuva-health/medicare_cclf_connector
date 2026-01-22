-- CTE that selects from either the source table or the demo data seed based on the 'demo_data_only' variable
with beneficiary_demographics as (
  SELECT 
    * 
  FROM
  {% if var('demo_data_only', false) %} {{ ref('beneficiary_demographics') }} {% else %} {{ source('medicare_cclf','beneficiary_demographics') }}{% endif %}
)

select
      BENE_MBI_ID
    , BENE_HIC_NUM
    , BENE_FIPS_STATE_CD
    , BENE_FIPS_CNTY_CD
    , BENE_ZIP_CD
    , BENE_DOB
    , BENE_SEX_CD
    , BENE_RACE_CD
    , BENE_AGE
    , BENE_MDCR_STUS_CD
    , BENE_DUAL_STUS_CD
    , BENE_DEATH_DT
    , BENE_RNG_BGN_DT
    , BENE_RNG_END_DT
    , BENE_1ST_NAME
    , BENE_MIDL_NAME
    , BENE_LAST_NAME
    , BENE_ORGNL_ENTLMT_RSN_CD
    , BENE_ENTLMT_BUYIN_IND
    , BENE_PART_A_ENRLMT_BGN_DT
    , BENE_PART_B_ENRLMT_BGN_DT
    , BENE_LINE_1_ADR
    , BENE_LINE_2_ADR
    , BENE_LINE_3_ADR
    , BENE_LINE_4_ADR
    , BENE_LINE_5_ADR
    , BENE_LINE_6_ADR
    , GEO_ZIP_PLC_NAME
    , GEO_USPS_STATE_CD
    , GEO_ZIP5_CD
    , GEO_ZIP4_CD
    , FILE_NAME
    , CAST(FILE_DATE AS date) AS FILE_DATE
from beneficiary_demographics