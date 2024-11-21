with staged_data as (

    select
          cur_clm_uniq_id
        , clm_line_num
        , bene_mbi_id
        , bene_hic_num
        , clm_type_cd
        , clm_line_from_dt
        , clm_line_thru_dt
        , clm_line_prod_rev_ctr_cd
        , clm_line_instnl_rev_ctr_dt
        , clm_line_hcpcs_cd
        , bene_eqtbl_bic_hicn_num
        , prvdr_oscar_num
        , clm_from_dt
        , clm_thru_dt
        , clm_line_srvc_unit_qty
        , clm_line_cvrd_pd_amt
        , hcpcs_1_mdfr_cd
        , hcpcs_2_mdfr_cd
        , hcpcs_3_mdfr_cd
        , hcpcs_4_mdfr_cd
        , hcpcs_5_mdfr_cd
        , clm_rev_apc_hipps_cd
        , file_name
        , file_date
    from {{ ref('stg_parta_claims_revenue_center_detail') }}

)

, beneficiary_xref as (

  select * from {{ ref('int_beneficiary_xref_deduped') }}

)

/* coalesce current MBI from XREF if exists and MBI on claim */
, add_current_mbi as (

    select
          cur_clm_uniq_id
        , clm_line_num
        , coalesce(beneficiary_xref.crnt_num, staged_data.bene_mbi_id) as current_bene_mbi_id
        , bene_hic_num
        , clm_type_cd
        , clm_line_from_dt
        , clm_line_thru_dt
        , clm_line_prod_rev_ctr_cd
        , clm_line_instnl_rev_ctr_dt
        , clm_line_hcpcs_cd
        , bene_eqtbl_bic_hicn_num
        , prvdr_oscar_num
        , clm_from_dt
        , clm_thru_dt
        , clm_line_srvc_unit_qty
        , clm_line_cvrd_pd_amt
        , hcpcs_1_mdfr_cd
        , hcpcs_2_mdfr_cd
        , hcpcs_3_mdfr_cd
        , hcpcs_4_mdfr_cd
        , hcpcs_5_mdfr_cd
        , clm_rev_apc_hipps_cd
        , file_name
        , file_date
    from staged_data
        left join beneficiary_xref
            on staged_data.bene_mbi_id = beneficiary_xref.prvs_num

)

/* dedupe full rows that may appear in multiple files */
, add_row_num as (

    select *, row_number() over (
        partition by
              cur_clm_uniq_id
            , clm_line_num
            , current_bene_mbi_id
            , bene_hic_num
            , clm_type_cd
            , clm_line_from_dt
            , clm_line_thru_dt
            , clm_line_prod_rev_ctr_cd
            , clm_line_instnl_rev_ctr_dt
            , clm_line_hcpcs_cd
            , bene_eqtbl_bic_hicn_num
            , prvdr_oscar_num
            , clm_from_dt
            , clm_thru_dt
            , clm_line_srvc_unit_qty
            , clm_line_cvrd_pd_amt
            , hcpcs_1_mdfr_cd
            , hcpcs_2_mdfr_cd
            , hcpcs_3_mdfr_cd
            , hcpcs_4_mdfr_cd
            , hcpcs_5_mdfr_cd
            , clm_rev_apc_hipps_cd
        order by file_date desc
        ) as row_num
    from add_current_mbi

)

, dedupe as (

    select
          cur_clm_uniq_id
        , clm_line_num
        , current_bene_mbi_id
        , bene_hic_num
        , clm_type_cd
        , clm_line_from_dt
        , clm_line_thru_dt
        , clm_line_prod_rev_ctr_cd
        , clm_line_instnl_rev_ctr_dt
        , clm_line_hcpcs_cd
        , bene_eqtbl_bic_hicn_num
        , prvdr_oscar_num
        , clm_from_dt
        , clm_thru_dt
        , clm_line_srvc_unit_qty
        , clm_line_cvrd_pd_amt
        , hcpcs_1_mdfr_cd
        , hcpcs_2_mdfr_cd
        , hcpcs_3_mdfr_cd
        , hcpcs_4_mdfr_cd
        , hcpcs_5_mdfr_cd
        , clm_rev_apc_hipps_cd
        , file_name
        , file_date
    from add_row_num
    where row_num = 1

)

select distinct
      cur_clm_uniq_id
    , clm_line_num
    , current_bene_mbi_id
    , bene_hic_num
    , clm_type_cd
    , clm_line_from_dt
    , clm_line_thru_dt
    , clm_line_prod_rev_ctr_cd
    , clm_line_instnl_rev_ctr_dt
    , clm_line_hcpcs_cd
    , bene_eqtbl_bic_hicn_num
    , prvdr_oscar_num
    , clm_from_dt
    , clm_thru_dt
    , clm_line_srvc_unit_qty
    , clm_line_cvrd_pd_amt
    , hcpcs_1_mdfr_cd
    , hcpcs_2_mdfr_cd
    , hcpcs_3_mdfr_cd
    , hcpcs_4_mdfr_cd
    , hcpcs_5_mdfr_cd
    , clm_rev_apc_hipps_cd
    , file_name
    , file_date
from dedupe