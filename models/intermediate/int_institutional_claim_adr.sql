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
    identify the final action claim(s) in each related set of Part A claims.

    A related set is every claim sharing the Part A natural key (CCLF IP v43 5.1.2):
     - CLM_BLG_PRVDR_OSCAR_NUM
     - CLM_FROM_DT
     - CLM_THRU_DT
     - Most Recent MBI

    IP 5.2.1 defines the final action claims as what remains once each cancellation claim
    (CLM_ADJSMT_TYPE_CD 1) is matched with an original (0) or adjustment (2) claim and every
    matched pair is removed. A related set can hold more than one final action claim (two
    originals, or the IP 5.3.2 Table 4 example), and a cancellation is never one. Neither
    CLM_EFCTV_DT nor CUR_CLM_UNIQ_ID identifies the final claim: a cancellation and its
    replacement adjustment are often processed together and share CLM_EFCTV_DT, and claim
    IDs carry no ordering meaning. So the related set is not ranked to a single winner.

    Matching rule. A cancellation is generated identical to the claim it cancels (IP 5.2.1),
    so cancellations are matched on CLM_PMT_AMT. Within a related set, the n cancellations
    carrying a given payment amount cancel the n oldest original/adjustment claims carrying
    that amount. Oldest is ordered by:
     1. CLM_EFCTV_DT ascending. A missing date (1000-01-01 / 9999-12-31, IP 3.6, cast to
        null) ranks first, as the oldest. A separate is-null sort key places it, because
        DuckDB and Snowflake put nulls at opposite ends of a sort by default;
     2. the delivering file's date ascending, a missing file date first;
     3. CUR_CLM_UNIQ_ID ascending. This key only orders claims that agree on payment,
        effective date and delivery, which the IP treats as interchangeable ("you will
        simply need to pick one", 5.3.2); it keeps that pick stable from run to run.

    A cancellation with no same-amount claim in its related set removes nothing and is
    itself dropped. That happens when the claim it cancels predates the loaded files, or
    when a corrected through date moved the replacement adjustment to another natural key
    (IP 5.2.1). Cancellation amounts are made negative for beneficiary-level debit/credit
    totals (IP 5.3.1); a final action claim keeps its own payment.
*/
, matching_input as (

    select
          *
        , {{ try_to_cast_numeric('clm_pmt_amt') }} as match_pmt_amt
        , case when clm_adjsmt_type_cd = '1' then 1 else 0 end as is_cancellation
        , {{ try_to_cast_date('clm_efctv_dt') }} as match_efctv_dt
    from normalized_data
    where normalized_row_num = 1

)

, cancellation_matching as (

    select
          *
        , sum(is_cancellation) over (
            partition by
                  clm_blg_prvdr_oscar_num
                , clm_from_dt
                , clm_thru_dt
                , current_bene_mbi_id
                , match_pmt_amt
          ) as cancellations_at_amount
        , row_number() over (
            partition by
                  clm_blg_prvdr_oscar_num
                , clm_from_dt
                , clm_thru_dt
                , current_bene_mbi_id
                , match_pmt_amt
                , is_cancellation
            order by
                  case when match_efctv_dt is null then 0 else 1 end
                , match_efctv_dt
                , case when file_date is null then 0 else 1 end
                , file_date
                , cur_clm_uniq_id
          ) as amount_match_seq
    from matching_input

)

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
            when is_cancellation = 0 and amount_match_seq > cancellations_at_amount then 1
            else 0
          end as final_action_flag
    from cancellation_matching

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
    , final_action_flag
from sort_adjusted_claims
