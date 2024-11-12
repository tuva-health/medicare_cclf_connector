with staged_data as (

    select
          cur_clm_uniq_id
        , prvdr_oscar_num
        , bene_mbi_id
        , bene_hic_num
        , clm_type_cd
        , clm_from_dt
        , clm_thru_dt
        , clm_bill_fac_type_cd
        , clm_bill_clsfctn_cd
        , prncpl_dgns_cd
        , admtg_dgns_cd
        , clm_mdcr_npmt_rsn_cd
        , clm_pmt_amt
        , clm_nch_prmry_pyr_cd
        , prvdr_fac_fips_st_cd
        , bene_ptnt_stus_cd
        , dgns_drg_cd
        , clm_op_srvc_type_cd
        , fac_prvdr_npi_num
        , oprtg_prvdr_npi_num
        , atndg_prvdr_npi_num
        , othr_prvdr_npi_num
        , clm_adjsmt_type_cd
        , clm_efctv_dt
        , clm_idr_ld_dt
        , bene_eqtbl_bic_hicn_num
        , clm_admsn_type_cd
        , clm_admsn_src_cd
        , clm_bill_freq_cd
        , clm_query_cd
        , dgns_prcdr_icd_ind
        , clm_mdcr_instnl_tot_chrg_amt
        , clm_mdcr_ip_pps_cptl_ime_amt
        , clm_oprtnl_ime_amt
        , clm_mdcr_ip_pps_dsprprtnt_amt
        , clm_hipps_uncompd_care_amt
        , clm_oprtnl_dsprprtnt_amt
        , clm_blg_prvdr_oscar_num
        , clm_blg_prvdr_npi_num
        , clm_oprtg_prvdr_npi_num
        , clm_atndg_prvdr_npi_num
        , clm_othr_prvdr_npi_num
        , clm_cntl_num
        , clm_org_cntl_num
        , clm_cntrctr_num
        , file_name
        , file_date
    from {{ ref('stg_parta_claims_header') }}

)

, beneficiary_xref as (

  select * from {{ ref('int_beneficiary_xref_deduped') }}

)

/*
    dedupe full rows that may appear in multiple files
*/
, add_row_num as (

    select *, row_number() over (
        partition by
              cur_clm_uniq_id
            , prvdr_oscar_num
            , bene_mbi_id
            , bene_hic_num
            , clm_type_cd
            , clm_from_dt
            , clm_thru_dt
            , clm_bill_fac_type_cd
            , clm_bill_clsfctn_cd
            , prncpl_dgns_cd
            , admtg_dgns_cd
            , clm_mdcr_npmt_rsn_cd
            , clm_pmt_amt
            , clm_nch_prmry_pyr_cd
            , prvdr_fac_fips_st_cd
            , bene_ptnt_stus_cd
            , dgns_drg_cd
            , clm_op_srvc_type_cd
            , fac_prvdr_npi_num
            , oprtg_prvdr_npi_num
            , atndg_prvdr_npi_num
            , othr_prvdr_npi_num
            , clm_adjsmt_type_cd
            , clm_efctv_dt
            , clm_idr_ld_dt
            , bene_eqtbl_bic_hicn_num
            , clm_admsn_type_cd
            , clm_admsn_src_cd
            , clm_bill_freq_cd
            , clm_query_cd
            , dgns_prcdr_icd_ind
            , clm_mdcr_instnl_tot_chrg_amt
            , clm_mdcr_ip_pps_cptl_ime_amt
            , clm_oprtnl_ime_amt
            , clm_mdcr_ip_pps_dsprprtnt_amt
            , clm_hipps_uncompd_care_amt
            , clm_oprtnl_dsprprtnt_amt
            , clm_blg_prvdr_oscar_num
            , clm_blg_prvdr_npi_num
            , clm_oprtg_prvdr_npi_num
            , clm_atndg_prvdr_npi_num
            , clm_othr_prvdr_npi_num
            , clm_cntl_num
            , clm_org_cntl_num
            , clm_cntrctr_num
        order by file_date desc
        ) as row_num
    from staged_data

)

/*
    source fields not mapped or used for adjustment logic are commented out
*/
, dedupe as (

    select
          cur_clm_uniq_id as cur_clm_uniq_id
        /*, prvdr_oscar_num*/
        , bene_mbi_id
        /*, bene_hic_num*/
        /*, clm_type_cd*/
        , clm_from_dt
        , clm_thru_dt
        , clm_bill_fac_type_cd
        , clm_bill_clsfctn_cd
        /*, prncpl_dgns_cd*/
        /*, admtg_dgns_cd*/
        /*, clm_mdcr_npmt_rsn_cd*/
        , clm_pmt_amt
        /*, clm_nch_prmry_pyr_cd*/
        /*, prvdr_fac_fips_st_cd*/
        , bene_ptnt_stus_cd
        , dgns_drg_cd
        /*, clm_op_srvc_type_cd*/
        , fac_prvdr_npi_num
        /*, oprtg_prvdr_npi_num*/
        , atndg_prvdr_npi_num
        /*, othr_prvdr_npi_num*/
        , clm_adjsmt_type_cd
        , clm_efctv_dt
        /*, clm_idr_ld_dt*/
        /*, bene_eqtbl_bic_hicn_num*/
        , clm_admsn_type_cd
        , clm_admsn_src_cd
        , clm_bill_freq_cd
        /*, clm_query_cd*/
        , dgns_prcdr_icd_ind
        , clm_mdcr_instnl_tot_chrg_amt
        /*, clm_mdcr_ip_pps_cptl_ime_amt*/
        /*, clm_oprtnl_ime_amt*/
        /*, clm_mdcr_ip_pps_dsprprtnt_amt*/
        /*, clm_hipps_uncompd_care_amt*/
        /*, clm_oprtnl_dsprprtnt_amt*/
        , clm_blg_prvdr_oscar_num
        /*, clm_blg_prvdr_npi_num*/
        /*, clm_oprtg_prvdr_npi_num*/
        /*, clm_atndg_prvdr_npi_num*/
        /*, clm_othr_prvdr_npi_num*/
        /*, clm_cntl_num*/
        /*, clm_org_cntl_num*/
        /*, clm_cntrctr_num*/
        , file_name
        , file_date
    from add_row_num
    where row_num = 1

)

/* coalesce current MBI from XREF if exists and MBI on claim */
, add_current_mbi as (

    select
          dedupe.cur_clm_uniq_id
        , coalesce(beneficiary_xref.crnt_num, dedupe.bene_mbi_id) as current_bene_mbi_id
        , dedupe.clm_from_dt
        , dedupe.clm_thru_dt
        , dedupe.clm_bill_fac_type_cd
        , dedupe.clm_bill_clsfctn_cd
        , dedupe.clm_pmt_amt
        , dedupe.bene_ptnt_stus_cd
        , dedupe.dgns_drg_cd
        , dedupe.fac_prvdr_npi_num
        , dedupe.atndg_prvdr_npi_num
        , dedupe.clm_adjsmt_type_cd
        , dedupe.clm_efctv_dt
        , dedupe.clm_admsn_type_cd
        , dedupe.clm_admsn_src_cd
        , dedupe.clm_bill_freq_cd
        , dedupe.dgns_prcdr_icd_ind
        , dedupe.clm_mdcr_instnl_tot_chrg_amt
        , dedupe.clm_blg_prvdr_oscar_num
        , dedupe.file_name
        , dedupe.file_date
    from dedupe
        left join beneficiary_xref
            on dedupe.bene_mbi_id = beneficiary_xref.prvs_num


)

/*
    1) apply adjustment logic by grouping part A claims by their natural keys:
     - CLM_BLG_PRVDR_OSCAR_NUM
     - CLM_FROM_DT
     - CLM_THRU_DT
     - Most Recent MBI

    2) sort grouped claims by the latest CLM_EFCTV_DT and CUR_CLM_UNIQ_ID since CLM_ADJSMT_TYPE_CD
    is not used consistently to indciate the latest final version of an adjusted claim.

    3) change paid amounts to negative for canceled claims

    (CCLF docs ref: 5.3 Calculating Beneficiary-Level Expenditures)
*/
, sort_adjusted_claims as (

    select
          cur_clm_uniq_id
        , current_bene_mbi_id
        , clm_from_dt
        , clm_thru_dt
        , clm_bill_fac_type_cd
        , clm_bill_clsfctn_cd
        , case
            when clm_adjsmt_type_cd = '1' then {{ cast_numeric('clm_pmt_amt') }} * -1
            else {{ cast_numeric('clm_pmt_amt') }}
          end as clm_pmt_amt
        , bene_ptnt_stus_cd
        , dgns_drg_cd
        , fac_prvdr_npi_num
        , atndg_prvdr_npi_num
        , clm_adjsmt_type_cd
        , clm_efctv_dt
        , clm_admsn_type_cd
        , clm_admsn_src_cd
        , clm_bill_freq_cd
        , dgns_prcdr_icd_ind
        , case
            when clm_adjsmt_type_cd = '1' then {{ cast_numeric('clm_mdcr_instnl_tot_chrg_amt') }} * -1
            else {{ cast_numeric('clm_mdcr_instnl_tot_chrg_amt') }}
          end as clm_mdcr_instnl_tot_chrg_amt
        , clm_blg_prvdr_oscar_num
        , file_name
        , file_date
        , row_number() over (
            partition by
                  clm_blg_prvdr_oscar_num
                , clm_from_dt
                , clm_thru_dt
                , current_bene_mbi_id
            order by
                  clm_efctv_dt desc
                , cur_clm_uniq_id desc
        ) as row_num
    from add_current_mbi

)

select
      cur_clm_uniq_id
    , current_bene_mbi_id
    , clm_from_dt
    , clm_thru_dt
    , clm_bill_fac_type_cd
    , clm_bill_clsfctn_cd
    , clm_pmt_amt
    , bene_ptnt_stus_cd
    , dgns_drg_cd
    , fac_prvdr_npi_num
    , atndg_prvdr_npi_num
    , clm_adjsmt_type_cd
    , clm_efctv_dt
    , clm_admsn_type_cd
    , clm_admsn_src_cd
    , clm_bill_freq_cd
    , dgns_prcdr_icd_ind
    , clm_mdcr_instnl_tot_chrg_amt
    , clm_blg_prvdr_oscar_num
    , file_name
    , file_date
    , row_num
from sort_adjusted_claims