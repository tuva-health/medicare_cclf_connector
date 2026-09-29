-- CTE that selects from either the source table or the demo data seed based on the 'demo_data_only' variable
with partb_demo_codes as (
  SELECT
    * 
  FROM
  {% if var('demo_data_only', false) %} {{ ref('partb_demo_codes') }} {% else %} {{ source('medicare_cclf','partb_demo_codes') }}{% endif %}
)

select
      cur_clm_uniq_id
    , clm_line_num
    , bene_mbi_id
    , bene_hic_num
    , clm_type_cd
    , clm_line_ngaco_pbpmt_sw
    , clm_line_ngaco_pdschrg_hcbs_sw
    , clm_line_ngaco_snf_wvr_sw
    , clm_line_ngaco_tlhlth_sw
    , clm_line_ngaco_cptatn_sw
    , clm_demo_1st_num
    , clm_demo_2nd_num
    , clm_demo_3rd_num
    , clm_demo_4th_num
    , clm_demo_5th_num
    , {{ try_to_cast_numeric('clm_pbp_inclsn_amt') }} as clm_pbp_inclsn_amt
    , {{ try_to_cast_numeric('clm_pbp_rdctn_amt') }} as clm_pbp_rdctn_amt
    , clm_ngaco_cmg_wvr_sw
    , {{ try_to_cast_numeric('clm_mdcr_ddctbl_amt') }} as clm_mdcr_ddctbl_amt
    , {{ try_to_cast_numeric('clm_sqstrtn_rdctn_amt') }} as clm_sqstrtn_rdctn_amt
    , clm_line_carr_hpsa_scrcty_cd
    , file_name
    , {{ try_to_cast_date('file_date') }} as file_date
from partb_demo_codes
