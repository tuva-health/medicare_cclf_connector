-- CTE that selects from either the source table or the demo data seed based on the 'demo_data_only' variable.
-- The CCLFA file is optional; set the 'demo_codes_enabled' variable to false if it is not
-- available and this model will produce an empty table with the expected columns.
with parta_demo_codes as (
{% if var('demo_data_only', false) %}
  select * from {{ ref('parta_demo_codes') }}
{% elif var('demo_codes_enabled', true) %}
  select * from {{ source('medicare_cclf','parta_demo_codes') }}
{% else %}
  select
          cast(null as {{ dbt.type_string() }}) as cur_clm_uniq_id
        , cast(null as {{ dbt.type_string() }}) as bene_mbi_id
        , cast(null as {{ dbt.type_string() }}) as bene_hic_num
        , cast(null as {{ dbt.type_string() }}) as clm_type_cd
        , cast(null as date) as clm_actv_care_from_dt
        , cast(null as {{ dbt.type_string() }}) as clm_ngaco_pbpmt_sw
        , cast(null as {{ dbt.type_string() }}) as clm_ngaco_pdschrg_hcbs_sw
        , cast(null as {{ dbt.type_string() }}) as clm_ngaco_snf_wvr_sw
        , cast(null as {{ dbt.type_string() }}) as clm_ngaco_tlhlth_sw
        , cast(null as {{ dbt.type_string() }}) as clm_ngaco_cptatn_sw
        , cast(null as {{ dbt.type_string() }}) as clm_demo_1st_num
        , cast(null as {{ dbt.type_string() }}) as clm_demo_2nd_num
        , cast(null as {{ dbt.type_string() }}) as clm_demo_3rd_num
        , cast(null as {{ dbt.type_string() }}) as clm_demo_4th_num
        , cast(null as {{ dbt.type_string() }}) as clm_demo_5th_num
        , cast(null as {{ dbt.type_numeric() }}) as clm_pbp_inclsn_amt
        , cast(null as {{ dbt.type_numeric() }}) as clm_pbp_rdctn_amt
        , cast(null as {{ dbt.type_string() }}) as clm_ngaco_cmg_wvr_sw
        , cast(null as {{ dbt.type_numeric() }}) as clm_instnl_per_diem_amt
        , cast(null as {{ dbt.type_numeric() }}) as clm_mdcr_ip_bene_ddctbl_amt
        , cast(null as {{ dbt.type_numeric() }}) as clm_mdcr_coinsrnc_amt
        , cast(null as {{ dbt.type_numeric() }}) as clm_blood_lblty_amt
        , cast(null as {{ dbt.type_numeric() }}) as clm_instnl_prfnl_amt
        , cast(null as {{ dbt.type_numeric() }}) as clm_ncvrd_chrg_amt
        , cast(null as {{ dbt.type_numeric() }}) as clm_mdcr_ddctbl_amt
        , cast(null as {{ dbt.type_string() }}) as clm_rlt_cond_cd
        , cast(null as {{ dbt.type_numeric() }}) as clm_oprtnl_outlr_amt
        , cast(null as {{ dbt.type_numeric() }}) as clm_mdcr_new_tech_amt
        , cast(null as {{ dbt.type_numeric() }}) as clm_islet_isoln_amt
        , cast(null as {{ dbt.type_numeric() }}) as clm_sqstrtn_rdctn_amt
        , cast(null as {{ dbt.type_string() }}) as clm_1_rev_cntr_ansi_rsn_cd
        , cast(null as {{ dbt.type_string() }}) as clm_1_rev_cntr_ansi_grp_cd
        , cast(null as {{ dbt.type_numeric() }}) as clm_mips_pmt_amt
        , cast(null as {{ dbt.type_string() }}) as file_name
        , cast(null as date) as file_date
  where 1 = 0
{% endif %}
)

select
      cur_clm_uniq_id
    , bene_mbi_id
    , bene_hic_num
    , clm_type_cd
    , {{ try_to_cast_date('clm_actv_care_from_dt') }} as clm_actv_care_from_dt
    , clm_ngaco_pbpmt_sw
    , clm_ngaco_pdschrg_hcbs_sw
    , clm_ngaco_snf_wvr_sw
    , clm_ngaco_tlhlth_sw
    , clm_ngaco_cptatn_sw
    , clm_demo_1st_num
    , clm_demo_2nd_num
    , clm_demo_3rd_num
    , clm_demo_4th_num
    , clm_demo_5th_num
    , {{ try_to_cast_numeric('clm_pbp_inclsn_amt') }} as clm_pbp_inclsn_amt
    , {{ try_to_cast_numeric('clm_pbp_rdctn_amt') }} as clm_pbp_rdctn_amt
    , clm_ngaco_cmg_wvr_sw
    , {{ try_to_cast_numeric('clm_instnl_per_diem_amt') }} as clm_instnl_per_diem_amt
    , {{ try_to_cast_numeric('clm_mdcr_ip_bene_ddctbl_amt') }} as clm_mdcr_ip_bene_ddctbl_amt
    , {{ try_to_cast_numeric('clm_mdcr_coinsrnc_amt') }} as clm_mdcr_coinsrnc_amt
    , {{ try_to_cast_numeric('clm_blood_lblty_amt') }} as clm_blood_lblty_amt
    , {{ try_to_cast_numeric('clm_instnl_prfnl_amt') }} as clm_instnl_prfnl_amt
    , {{ try_to_cast_numeric('clm_ncvrd_chrg_amt') }} as clm_ncvrd_chrg_amt
    , {{ try_to_cast_numeric('clm_mdcr_ddctbl_amt') }} as clm_mdcr_ddctbl_amt
    , clm_rlt_cond_cd
    , {{ try_to_cast_numeric('clm_oprtnl_outlr_amt') }} as clm_oprtnl_outlr_amt
    , {{ try_to_cast_numeric('clm_mdcr_new_tech_amt') }} as clm_mdcr_new_tech_amt
    , {{ try_to_cast_numeric('clm_islet_isoln_amt') }} as clm_islet_isoln_amt
    , {{ try_to_cast_numeric('clm_sqstrtn_rdctn_amt') }} as clm_sqstrtn_rdctn_amt
    , clm_1_rev_cntr_ansi_rsn_cd
    , clm_1_rev_cntr_ansi_grp_cd
    , {{ try_to_cast_numeric('clm_mips_pmt_amt') }} as clm_mips_pmt_amt
    , file_name
    , {{ try_to_cast_date('file_date') }} as file_date
from parta_demo_codes
