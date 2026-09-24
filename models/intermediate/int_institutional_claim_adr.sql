with staged_data as (

    select
          cast(cur_clm_uniq_id as {{ dbt.type_string() }}) as cur_clm_uniq_id
        , cast(prvdr_oscar_num as {{ dbt.type_string() }}) as prvdr_oscar_num
        , cast(bene_mbi_id as {{ dbt.type_string() }}) as bene_mbi_id
        , cast(bene_hic_num as {{ dbt.type_string() }}) as bene_hic_num
        , cast(clm_type_cd as {{ dbt.type_string() }}) as clm_type_cd
        , cast(clm_from_dt as {{ dbt.type_string() }}) as clm_from_dt
        , cast(clm_thru_dt as {{ dbt.type_string() }}) as clm_thru_dt
        , cast(clm_bill_fac_type_cd as {{ dbt.type_string() }}) as clm_bill_fac_type_cd
        , cast(clm_bill_clsfctn_cd as {{ dbt.type_string() }}) as clm_bill_clsfctn_cd
        , cast(prncpl_dgns_cd as {{ dbt.type_string() }}) as prncpl_dgns_cd
        , cast(admtg_dgns_cd as {{ dbt.type_string() }}) as admtg_dgns_cd
        , cast(nullif(clm_mdcr_npmt_rsn_cd, '~') as {{ dbt.type_string() }}) as clm_mdcr_npmt_rsn_cd
        , cast(clm_pmt_amt as {{ dbt.type_string() }}) as clm_pmt_amt
        , cast(clm_nch_prmry_pyr_cd as {{ dbt.type_string() }}) as clm_nch_prmry_pyr_cd
        , cast(prvdr_fac_fips_st_cd as {{ dbt.type_string() }}) as prvdr_fac_fips_st_cd
        , cast(bene_ptnt_stus_cd as {{ dbt.type_string() }}) as bene_ptnt_stus_cd
        , cast(dgns_drg_cd as {{ dbt.type_string() }}) as dgns_drg_cd
        , cast(clm_op_srvc_type_cd as {{ dbt.type_string() }}) as clm_op_srvc_type_cd
        , cast(fac_prvdr_npi_num as {{ dbt.type_string() }}) as fac_prvdr_npi_num
        , cast(oprtg_prvdr_npi_num as {{ dbt.type_string() }}) as oprtg_prvdr_npi_num
        , cast(atndg_prvdr_npi_num as {{ dbt.type_string() }}) as atndg_prvdr_npi_num
        , cast(othr_prvdr_npi_num as {{ dbt.type_string() }}) as othr_prvdr_npi_num
        , cast(clm_adjsmt_type_cd as {{ dbt.type_string() }}) as clm_adjsmt_type_cd
        , cast(clm_efctv_dt as {{ dbt.type_string() }}) as clm_efctv_dt
        , cast(clm_idr_ld_dt as {{ dbt.type_string() }}) as clm_idr_ld_dt
        , cast(bene_eqtbl_bic_hicn_num as {{ dbt.type_string() }}) as bene_eqtbl_bic_hicn_num
        , cast(clm_admsn_type_cd as {{ dbt.type_string() }}) as clm_admsn_type_cd
        , cast(clm_admsn_src_cd as {{ dbt.type_string() }}) as clm_admsn_src_cd
        , cast(clm_bill_freq_cd as {{ dbt.type_string() }}) as clm_bill_freq_cd
        , cast(clm_query_cd as {{ dbt.type_string() }}) as clm_query_cd
        , cast(dgns_prcdr_icd_ind as {{ dbt.type_string() }}) as dgns_prcdr_icd_ind
        , cast(clm_mdcr_instnl_tot_chrg_amt as {{ dbt.type_string() }}) as clm_mdcr_instnl_tot_chrg_amt
        , cast(clm_mdcr_ip_pps_cptl_ime_amt as {{ dbt.type_string() }}) as clm_mdcr_ip_pps_cptl_ime_amt
        , cast(clm_oprtnl_ime_amt as {{ dbt.type_string() }}) as clm_oprtnl_ime_amt
        , cast(clm_mdcr_ip_pps_dsprprtnt_amt as {{ dbt.type_string() }}) as clm_mdcr_ip_pps_dsprprtnt_amt
        , cast(clm_hipps_uncompd_care_amt as {{ dbt.type_string() }}) as clm_hipps_uncompd_care_amt
        , cast(clm_oprtnl_dsprprtnt_amt as {{ dbt.type_string() }}) as clm_oprtnl_dsprprtnt_amt
        , cast(clm_blg_prvdr_oscar_num as {{ dbt.type_string() }}) as clm_blg_prvdr_oscar_num
        , cast(clm_blg_prvdr_npi_num as {{ dbt.type_string() }}) as clm_blg_prvdr_npi_num
        , cast(clm_oprtg_prvdr_npi_num as {{ dbt.type_string() }}) as clm_oprtg_prvdr_npi_num
        , cast(clm_atndg_prvdr_npi_num as {{ dbt.type_string() }}) as clm_atndg_prvdr_npi_num
        , cast(clm_othr_prvdr_npi_num as {{ dbt.type_string() }}) as clm_othr_prvdr_npi_num
        , cast(clm_cntl_num as {{ dbt.type_string() }}) as clm_cntl_num
        , cast(clm_org_cntl_num as {{ dbt.type_string() }}) as clm_org_cntl_num
        , cast(clm_cntrctr_num as {{ dbt.type_string() }}) as clm_cntrctr_num
        , cast(current_bene_mbi_id as {{ dbt.type_string() }}) as current_bene_mbi_id
        , file_name
        , file_date
    from {{ ref('int_parta_claims_header_normalized') }}

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
        , prvdr_oscar_num as ccn
        , bene_mbi_id
        , current_bene_mbi_id
        /*, bene_hic_num*/
        , clm_type_cd
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
        , oprtg_prvdr_npi_num
        , atndg_prvdr_npi_num
        , othr_prvdr_npi_num
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
        -- Exclude unpaid claims
        and nullif(trim(clm_mdcr_npmt_rsn_cd),'') is null
)

/*
    dedupe re-delivered copies of the same claim version. CMS re-delivers claims across
    monthly files and later file layouts blank columns such as HIC and BETOS, so a full-row
    partition cannot collapse them. A claim version is identified by its claim ID,
    adjustment type and effective date; the most recently delivered copy wins.
*/
, normalized_data as (

    select *,
        row_number() over (
            partition by
                  cur_clm_uniq_id
                , clm_adjsmt_type_cd
                , clm_efctv_dt
            order by file_date desc
        ) as normalized_row_num
    from dedupe

)

/*
    flag related sets (natural-key groups) that contain a cancellation (1) or adjustment (2).
    A related set made up only of original claims is a set of distinct final action claims
    (CCLF IP 5.2: "it is possible that there is more than one final action claim among a
    related set of claims"), so those must not be collapsed into one claim. The adjustment
    key falls back to CUR_CLM_UNIQ_ID for original-only sets and is a constant otherwise.
*/
, flag_adjusted_groups as (

    select
          *
        , max(case when clm_adjsmt_type_cd in ('1', '2') then 1 else 0 end) over (
            partition by
                  clm_blg_prvdr_oscar_num
                , clm_from_dt
                , clm_thru_dt
                , current_bene_mbi_id
          ) as group_has_adjustment
    from normalized_data
    where normalized_row_num = 1

)

/*
    1) apply adjustment logic by grouping part A claims by their natural keys:
     - CLM_BLG_PRVDR_OSCAR_NUM
     - CLM_FROM_DT
     - CLM_THRU_DT
     - Most Recent MBI
     - adjustment_key (CUR_CLM_UNIQ_ID when the related set holds only original claims,
       so each original stays its own claim; constant otherwise)

    2) sort grouped claims by the latest CLM_EFCTV_DT and CUR_CLM_UNIQ_ID since CLM_ADJSMT_TYPE_CD
    is not used consistently to indicate the latest final version of an adjusted claim.

    3) change paid amounts to negative for canceled claims

    (CCLF docs ref: 5.3 Calculating Beneficiary-Level Expenditures)
*/
, sort_adjusted_claims as (

    select
          cur_clm_uniq_id
        , bene_mbi_id
        , current_bene_mbi_id
        , clm_from_dt
        , clm_thru_dt
        , clm_bill_fac_type_cd
        , clm_bill_clsfctn_cd
        , case
            when clm_adjsmt_type_cd = '1' then {{ try_to_cast_numeric('clm_pmt_amt') }} * -1
            else {{ try_to_cast_numeric('clm_pmt_amt') }}
          end as clm_pmt_amt
        , bene_ptnt_stus_cd
        , dgns_drg_cd
        , ccn
        , clm_type_cd        
        , fac_prvdr_npi_num
        , othr_prvdr_npi_num
        , atndg_prvdr_npi_num
        , oprtg_prvdr_npi_num     
        , clm_adjsmt_type_cd
        , clm_efctv_dt
        , clm_admsn_type_cd
        , clm_admsn_src_cd
        , clm_bill_freq_cd
        , dgns_prcdr_icd_ind
        , case
            when clm_adjsmt_type_cd = '1' then {{ try_to_cast_numeric('clm_mdcr_instnl_tot_chrg_amt') }} * -1
            else {{ try_to_cast_numeric('clm_mdcr_instnl_tot_chrg_amt') }}
          end as clm_mdcr_instnl_tot_chrg_amt
        , clm_blg_prvdr_oscar_num
        , file_name
        , file_date
        , case
            when group_has_adjustment = 1 then cast('' as {{ dbt.type_string() }})
            else cur_clm_uniq_id
          end as adjustment_key
        , row_number() over (
            partition by
                  clm_blg_prvdr_oscar_num
                , clm_from_dt
                , clm_thru_dt
                , current_bene_mbi_id
                , case
                    when group_has_adjustment = 1 then cast('' as {{ dbt.type_string() }})
                    else cur_clm_uniq_id
                  end
            order by
                  clm_efctv_dt desc
                , cur_clm_uniq_id desc
        ) as row_num
    from flag_adjusted_groups

)

select
      cur_clm_uniq_id
    , bene_mbi_id
    , current_bene_mbi_id
    , clm_from_dt
    , clm_thru_dt
    , clm_bill_fac_type_cd
    , clm_bill_clsfctn_cd
    , clm_pmt_amt
    , bene_ptnt_stus_cd
    , dgns_drg_cd
    , ccn
    , clm_type_cd    
    , fac_prvdr_npi_num
    , othr_prvdr_npi_num
    , atndg_prvdr_npi_num
    , oprtg_prvdr_npi_num  
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
    , adjustment_key
    , row_num
from sort_adjusted_claims
