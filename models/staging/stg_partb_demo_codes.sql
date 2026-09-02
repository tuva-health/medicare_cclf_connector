-- CTE that selects from either the source table or the demo data seed based on the 'demo_data_only' variable.
-- The CCLFB file is optional; set the 'demo_codes_enabled' variable to false if it is not
-- available and this model will return an empty result set with the expected columns.
with partb_demo_codes as (
{% if var('demo_data_only', false) %}
  select * from {{ ref('partb_demo_codes') }}
{% elif var('demo_codes_enabled', true) %}
  select * from {{ source('medicare_cclf','partb_demo_codes') }}
{% else %}
  select
          cast(null as {{ dbt.type_string() }}) as cur_clm_uniq_id
        , cast(null as {{ dbt.type_string() }}) as clm_line_num
        , cast(null as {{ dbt.type_string() }}) as bene_mbi_id
        , cast(null as {{ dbt.type_string() }}) as bene_hic_num
        , cast(null as {{ dbt.type_string() }}) as clm_type_cd
        , cast(null as {{ dbt.type_string() }}) as clm_line_ngaco_pbpmt_sw
        , cast(null as {{ dbt.type_string() }}) as clm_line_ngaco_pdschrg_hcbs_sw
        , cast(null as {{ dbt.type_string() }}) as clm_line_ngaco_snf_wvr_sw
        , cast(null as {{ dbt.type_string() }}) as clm_line_ngaco_tlhlth_sw
        , cast(null as {{ dbt.type_string() }}) as clm_line_ngaco_cptatn_sw
        , cast(null as {{ dbt.type_string() }}) as clm_demo_1st_num
        , cast(null as {{ dbt.type_string() }}) as clm_demo_2nd_num
        , cast(null as {{ dbt.type_string() }}) as clm_demo_3rd_num
        , cast(null as {{ dbt.type_string() }}) as clm_demo_4th_num
        , cast(null as {{ dbt.type_string() }}) as clm_demo_5th_num
        , cast(null as {{ dbt.type_numeric() }}) as clm_pbp_inclsn_amt
        , cast(null as {{ dbt.type_numeric() }}) as clm_pbp_rdctn_amt
        , cast(null as {{ dbt.type_string() }}) as clm_ngaco_cmg_wvr_sw
        , cast(null as {{ dbt.type_numeric() }}) as clm_mdcr_ddctbl_amt
        , cast(null as {{ dbt.type_numeric() }}) as clm_sqstrtn_rdctn_amt
        , cast(null as {{ dbt.type_string() }}) as clm_line_carr_hpsa_scrcty_cd
        , cast(null as {{ dbt.type_string() }}) as file_name
        , cast(null as date) as file_date
  where 1 = 0
{% endif %}
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
