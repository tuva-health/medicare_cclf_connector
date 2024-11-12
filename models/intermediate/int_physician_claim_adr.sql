with staged_data as (

    select
          cur_clm_uniq_id
        , clm_line_num
        , bene_mbi_id
        , bene_hic_num
        , clm_type_cd
        , clm_from_dt
        , clm_thru_dt
        , rndrg_prvdr_type_cd
        , rndrg_prvdr_fips_st_cd
        , clm_prvdr_spclty_cd
        , clm_fed_type_srvc_cd
        , clm_pos_cd
        , clm_line_from_dt
        , clm_line_thru_dt
        , clm_line_hcpcs_cd
        , clm_line_cvrd_pd_amt
        , clm_line_prmry_pyr_cd
        , clm_line_dgns_cd
        , clm_rndrg_prvdr_tax_num
        , rndrg_prvdr_npi_num
        , clm_carr_pmt_dnl_cd
        , clm_prcsg_ind_cd
        , clm_adjsmt_type_cd
        , clm_efctv_dt
        , clm_idr_ld_dt
        , clm_cntl_num
        , bene_eqtbl_bic_hicn_num
        , clm_line_alowd_chrg_amt
        , clm_line_srvc_unit_qty
        , hcpcs_1_mdfr_cd
        , hcpcs_2_mdfr_cd
        , hcpcs_3_mdfr_cd
        , hcpcs_4_mdfr_cd
        , hcpcs_5_mdfr_cd
        , clm_disp_cd
        , clm_dgns_1_cd
        , clm_dgns_2_cd
        , clm_dgns_3_cd
        , clm_dgns_4_cd
        , clm_dgns_5_cd
        , clm_dgns_6_cd
        , clm_dgns_7_cd
        , clm_dgns_8_cd
        , dgns_prcdr_icd_ind
        , clm_dgns_9_cd
        , clm_dgns_10_cd
        , clm_dgns_11_cd
        , clm_dgns_12_cd
        , hcpcs_betos_cd
        , file_name
        , file_date
    from {{ ref('stg_partb_physicians') }}

)

, beneficiary_xref as (

  select * from {{ ref('int_beneficiary_xref_deduped') }}

)

/* dedupe full rows that may appear in multiple files */
, add_row_num as (

    select *, row_number() over (
        partition by
              cur_clm_uniq_id
            , clm_line_num
            , bene_mbi_id
            , bene_hic_num
            , clm_type_cd
            , clm_from_dt
            , clm_thru_dt
            , rndrg_prvdr_type_cd
            , rndrg_prvdr_fips_st_cd
            , clm_prvdr_spclty_cd
            , clm_fed_type_srvc_cd
            , clm_pos_cd
            , clm_line_from_dt
            , clm_line_thru_dt
            , clm_line_hcpcs_cd
            , clm_line_cvrd_pd_amt
            , clm_line_prmry_pyr_cd
            , clm_line_dgns_cd
            , clm_rndrg_prvdr_tax_num
            , rndrg_prvdr_npi_num
            , clm_carr_pmt_dnl_cd
            , clm_prcsg_ind_cd
            , clm_adjsmt_type_cd
            , clm_efctv_dt
            , clm_idr_ld_dt
            , clm_cntl_num
            , bene_eqtbl_bic_hicn_num
            , clm_line_alowd_chrg_amt
            , clm_line_srvc_unit_qty
            , hcpcs_1_mdfr_cd
            , hcpcs_2_mdfr_cd
            , hcpcs_3_mdfr_cd
            , hcpcs_4_mdfr_cd
            , hcpcs_5_mdfr_cd
            , clm_disp_cd
            , clm_dgns_1_cd
            , clm_dgns_2_cd
            , clm_dgns_3_cd
            , clm_dgns_4_cd
            , clm_dgns_5_cd
            , clm_dgns_6_cd
            , clm_dgns_7_cd
            , clm_dgns_8_cd
            , dgns_prcdr_icd_ind
            , clm_dgns_9_cd
            , clm_dgns_10_cd
            , clm_dgns_11_cd
            , clm_dgns_12_cd
            , hcpcs_betos_cd
        order by file_date desc
        ) as row_num
    from staged_data

)

/* source fields not mapped or used for adjustment logic are commented out */
, dedupe as (

    select
          cur_clm_uniq_id
        , clm_line_num
        , bene_mbi_id
        /*, bene_hic_num*/
        /*, clm_type_cd*/
        , clm_from_dt
        , clm_thru_dt
        /*, rndrg_prvdr_type_cd*/
        /*, rndrg_prvdr_fips_st_cd*/
        /*, clm_prvdr_spclty_cd*/
        /*, clm_fed_type_srvc_cd*/
        , clm_pos_cd
        , clm_line_from_dt
        , clm_line_thru_dt
        , clm_line_hcpcs_cd
        , clm_line_cvrd_pd_amt
        /*, clm_line_prmry_pyr_cd*/
        /*, clm_line_dgns_cd*/
        , clm_rndrg_prvdr_tax_num
        , rndrg_prvdr_npi_num
        /*, clm_carr_pmt_dnl_cd*/
        /*, clm_prcsg_ind_cd*/
        , clm_adjsmt_type_cd
        , clm_efctv_dt
        /*, clm_idr_ld_dt*/
        , clm_cntl_num
        /*, bene_eqtbl_bic_hicn_num*/
        , clm_line_alowd_chrg_amt
        , clm_line_srvc_unit_qty
        , hcpcs_1_mdfr_cd
        , hcpcs_2_mdfr_cd
        , hcpcs_3_mdfr_cd
        , hcpcs_4_mdfr_cd
        , hcpcs_5_mdfr_cd
        /*, clm_disp_cd*/
        , clm_dgns_1_cd
        , clm_dgns_2_cd
        , clm_dgns_3_cd
        , clm_dgns_4_cd
        , clm_dgns_5_cd
        , clm_dgns_6_cd
        , clm_dgns_7_cd
        , clm_dgns_8_cd
        , dgns_prcdr_icd_ind
        , clm_dgns_9_cd
        , clm_dgns_10_cd
        , clm_dgns_11_cd
        , clm_dgns_12_cd
        /*, hcpcs_betos_cd*/
        , file_name
        , file_date
    from add_row_num
    where row_num = 1

)

/* coalesce current MBI from XREF if exists and MBI on claim */
, add_current_mbi as (

    select
          dedupe.cur_clm_uniq_id
        , dedupe.clm_line_num
        , coalesce(beneficiary_xref.crnt_num, dedupe.bene_mbi_id) as current_bene_mbi_id
        , dedupe.clm_from_dt
        , dedupe.clm_thru_dt
        , dedupe.clm_pos_cd
        , dedupe.clm_line_from_dt
        , dedupe.clm_line_thru_dt
        , dedupe.clm_line_hcpcs_cd
        , dedupe.clm_line_cvrd_pd_amt
        , dedupe.clm_rndrg_prvdr_tax_num
        , dedupe.rndrg_prvdr_npi_num
        , dedupe.clm_adjsmt_type_cd
        , dedupe.clm_efctv_dt
        , dedupe.clm_cntl_num
        , dedupe.clm_line_alowd_chrg_amt
        , dedupe.clm_line_srvc_unit_qty
        , dedupe.hcpcs_1_mdfr_cd
        , dedupe.hcpcs_2_mdfr_cd
        , dedupe.hcpcs_3_mdfr_cd
        , dedupe.hcpcs_4_mdfr_cd
        , dedupe.hcpcs_5_mdfr_cd
        , dedupe.clm_dgns_1_cd
        , dedupe.clm_dgns_2_cd
        , dedupe.clm_dgns_3_cd
        , dedupe.clm_dgns_4_cd
        , dedupe.clm_dgns_5_cd
        , dedupe.clm_dgns_6_cd
        , dedupe.clm_dgns_7_cd
        , dedupe.clm_dgns_8_cd
        , dedupe.dgns_prcdr_icd_ind
        , dedupe.clm_dgns_9_cd
        , dedupe.clm_dgns_10_cd
        , dedupe.clm_dgns_11_cd
        , dedupe.clm_dgns_12_cd
        , dedupe.file_name
        , dedupe.file_date
    from dedupe
        left join beneficiary_xref
            on dedupe.bene_mbi_id = beneficiary_xref.prvs_num


)

/*
    1) apply adjustment logic by grouping part B Physician claims by their natural keys:
     - CLM_CNTL_NUM
     - Most Recent MBI
     - CLM_LINE_NUM (not listed in CCLF docs, but used to prevent line detail loss)

    2) sort grouped claims by the latest CLM_EFCTV_DT and CUR_CLM_UNIQ_ID since CLM_ADJSMT_TYPE_CD
    is not used consistently to indicate the latest final version of an adjusted claim.

    3) change paid amounts to negative for canceled claims

    (CCLF docs ref: 5.3 Calculating Beneficiary-Level Expenditures)
*/
, sort_adjusted_claims as (

    select
          cur_clm_uniq_id
        , clm_line_num
        , current_bene_mbi_id
        , clm_from_dt
        , clm_thru_dt
        , clm_pos_cd
        , clm_line_from_dt
        , clm_line_thru_dt
        , clm_line_hcpcs_cd
        , case
            when clm_adjsmt_type_cd = '1' then {{ cast_numeric('clm_line_cvrd_pd_amt') }} * -1
            else {{ cast_numeric('clm_line_cvrd_pd_amt') }}
          end as clm_line_cvrd_pd_amt
        , clm_rndrg_prvdr_tax_num
        , rndrg_prvdr_npi_num
        , clm_adjsmt_type_cd
        , clm_efctv_dt
        , clm_cntl_num
        , case
            when clm_adjsmt_type_cd = '1' then {{ cast_numeric('clm_line_alowd_chrg_amt') }} * -1
            else {{ cast_numeric('clm_line_alowd_chrg_amt') }}
          end as clm_line_alowd_chrg_amt
        , clm_line_srvc_unit_qty
        , hcpcs_1_mdfr_cd
        , hcpcs_2_mdfr_cd
        , hcpcs_3_mdfr_cd
        , hcpcs_4_mdfr_cd
        , hcpcs_5_mdfr_cd
        , clm_dgns_1_cd
        , clm_dgns_2_cd
        , clm_dgns_3_cd
        , clm_dgns_4_cd
        , clm_dgns_5_cd
        , clm_dgns_6_cd
        , clm_dgns_7_cd
        , clm_dgns_8_cd
        , dgns_prcdr_icd_ind
        , clm_dgns_9_cd
        , clm_dgns_10_cd
        , clm_dgns_11_cd
        , clm_dgns_12_cd
        , file_name
        , file_date
        , row_number() over (
            partition by
                  clm_cntl_num
                , clm_line_num
                , current_bene_mbi_id
            order by
                  clm_efctv_dt desc
                , cur_clm_uniq_id desc
        ) as row_num
    from add_current_mbi

)

select
      cur_clm_uniq_id
    , clm_line_num
    , current_bene_mbi_id
    , clm_from_dt
    , clm_thru_dt
    , clm_pos_cd
    , clm_line_from_dt
    , clm_line_thru_dt
    , clm_line_hcpcs_cd
    , clm_line_cvrd_pd_amt
    , clm_rndrg_prvdr_tax_num
    , rndrg_prvdr_npi_num
    , clm_adjsmt_type_cd
    , clm_efctv_dt
    , clm_cntl_num
    , clm_line_alowd_chrg_amt
    , clm_line_srvc_unit_qty
    , hcpcs_1_mdfr_cd
    , hcpcs_2_mdfr_cd
    , hcpcs_3_mdfr_cd
    , hcpcs_4_mdfr_cd
    , hcpcs_5_mdfr_cd
    , clm_dgns_1_cd
    , clm_dgns_2_cd
    , clm_dgns_3_cd
    , clm_dgns_4_cd
    , clm_dgns_5_cd
    , clm_dgns_6_cd
    , clm_dgns_7_cd
    , clm_dgns_8_cd
    , dgns_prcdr_icd_ind
    , clm_dgns_9_cd
    , clm_dgns_10_cd
    , clm_dgns_11_cd
    , clm_dgns_12_cd
    , file_name
    , file_date
    , row_num
from sort_adjusted_claims