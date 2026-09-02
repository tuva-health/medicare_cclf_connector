{#
    CCLFA (Part A benefit enhancement and demonstration codes) is a claim-level
    file, but a claim can appear on several rows: one per related condition
    code (clm_rlt_cond_cd), and again in each monthly / run-out file. The
    payment amounts are identical on every row for a claim, so we keep one row
    per claim, preferring the most recent file.
#}

with demo_codes as (

    select
          cur_clm_uniq_id
        , clm_pbp_inclsn_amt
        , clm_pbp_rdctn_amt
        , clm_mdcr_ddctbl_amt
        , clm_sqstrtn_rdctn_amt
        , clm_islet_isoln_amt
        , clm_instnl_per_diem_amt
        , clm_mdcr_ip_bene_ddctbl_amt
        , clm_mdcr_coinsrnc_amt
        , clm_blood_lblty_amt
        , clm_instnl_prfnl_amt
        , clm_ncvrd_chrg_amt
        , clm_oprtnl_outlr_amt
        , clm_mdcr_new_tech_amt
        , clm_mips_pmt_amt
        , file_name
        , file_date
        , row_number() over (
            partition by cur_clm_uniq_id
            order by
                  file_date desc
                , file_name desc
          ) as row_num
    from {{ ref('stg_parta_demo_codes') }}
    where cur_clm_uniq_id is not null

)

select
      cast(cur_clm_uniq_id as {{ dbt.type_string() }}) as claim_id
    , {{ cast_numeric('clm_pbp_inclsn_amt') }} as clm_pbp_inclsn_amt
    , {{ cast_numeric('clm_pbp_rdctn_amt') }} as clm_pbp_rdctn_amt
    , {{ cast_numeric('clm_mdcr_ddctbl_amt') }} as clm_mdcr_ddctbl_amt
    , {{ cast_numeric('clm_sqstrtn_rdctn_amt') }} as clm_sqstrtn_rdctn_amt
    , {{ cast_numeric('clm_islet_isoln_amt') }} as clm_islet_isoln_amt
    , {{ cast_numeric('clm_instnl_per_diem_amt') }} as clm_instnl_per_diem_amt
    , {{ cast_numeric('clm_mdcr_ip_bene_ddctbl_amt') }} as clm_mdcr_ip_bene_ddctbl_amt
    , {{ cast_numeric('clm_mdcr_coinsrnc_amt') }} as clm_mdcr_coinsrnc_amt
    , {{ cast_numeric('clm_blood_lblty_amt') }} as clm_blood_lblty_amt
    , {{ cast_numeric('clm_instnl_prfnl_amt') }} as clm_instnl_prfnl_amt
    , {{ cast_numeric('clm_ncvrd_chrg_amt') }} as clm_ncvrd_chrg_amt
    , {{ cast_numeric('clm_oprtnl_outlr_amt') }} as clm_oprtnl_outlr_amt
    , {{ cast_numeric('clm_mdcr_new_tech_amt') }} as clm_mdcr_new_tech_amt
    , {{ cast_numeric('clm_mips_pmt_amt') }} as clm_mips_pmt_amt
    , file_name
    , file_date
from demo_codes
where row_num = 1
