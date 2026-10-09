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

    Matching rule (our choice where the IP leaves the pairing open):
     - A cancellation copies the claim it cancels (IP 5.2.1), so it only pairs with an
       original or adjustment of equal CLM_PMT_AMT in its related set.
     - An equal-amount original is always an eligible partner: an original is always
       processed before its cancellation (IP 5.2.1).
     - An equal-amount adjustment is eligible only if it strictly precedes the
       cancellation: an earlier CLM_EFCTV_DT, or, when the effective dates are equal or
       either is missing, an earlier file date. An adjustment with the same effective date
       and file date as the cancellation is never its partner. That is the replacement
       issued in the same action; when the original predates the loaded files and the
       replacement repeats its payment, pairing the two would drop the real final claim.
     - Cancellations take partners in age order, each taking the oldest eligible original
       first, else the oldest eligible adjustment.
     - Age order: CLM_EFCTV_DT, then file date, then CUR_CLM_UNIQ_ID, all ascending. A
       missing date (1000-01-01 / 9999-12-31, IP 3.6, cast to null) ranks first, as the
       oldest; an is-null sort key places it, because DuckDB and Snowflake put nulls at
       opposite ends of a sort by default. The claim ID only orders claims that agree on
       payment, effective date and delivery, which the IP treats as interchangeable ("you
       will simply need to pick one", 5.3.2); it keeps that pick stable from run to run.

    Because originals are always eligible and preferred, the first k cancellations of an
    amount take that amount's k oldest originals. The cancellations left over take
    adjustments. When effective dates are present, the adjustments eligible for a
    cancellation are a prefix of the adjustments in age order, and that prefix only grows
    for later cancellations. Cancelling in order then takes the adjustments in age order,
    and the number taken by the j-th leftover cancellation is
    j + min(0, min over i <= j of (eligible_i - i)). This set-based form reproduces the
    sequential rule. It can differ from it only when an adjustment or a cancellation of
    the same amount has no effective date; Part A effective dates carried no sentinel
    values in client profiling.

    A cancellation with no eligible partner removes nothing and is itself dropped. That
    happens when the claim it cancels predates the loaded files, or when a corrected
    through date moved the replacement adjustment to another natural key (IP 5.2.1).
    Cancellation amounts are made negative for beneficiary-level debit/credit totals
    (IP 5.3.1); a final action claim keeps its own payment.
*/
, matching_input as (

    select
          *
        , {{ try_to_cast_numeric('clm_pmt_amt') }} as match_pmt_amt
        , case
            when clm_adjsmt_type_cd = '1' then 'cancellation'
            when clm_adjsmt_type_cd = '0' then 'original'
            else 'adjustment'
          end as match_role
        , {{ try_to_cast_date('clm_efctv_dt') }} as match_efctv_dt
        -- a claim version is unique on these columns after normalized_data
        , cur_clm_uniq_id
            || '|' || coalesce(clm_adjsmt_type_cd, '')
            || '|' || coalesce(clm_efctv_dt, '') as match_version_key
    from normalized_data
    where normalized_row_num = 1

)

, match_sequences as (

    select
          *
        , sum(case when match_role = 'cancellation' then 1 else 0 end) over (
            partition by
                  clm_blg_prvdr_oscar_num
                , clm_from_dt
                , clm_thru_dt
                , current_bene_mbi_id
                , match_pmt_amt
          ) as cancellations_at_amount
        , sum(case when match_role = 'original' then 1 else 0 end) over (
            partition by
                  clm_blg_prvdr_oscar_num
                , clm_from_dt
                , clm_thru_dt
                , current_bene_mbi_id
                , match_pmt_amt
          ) as originals_at_amount
        , row_number() over (
            partition by
                  clm_blg_prvdr_oscar_num
                , clm_from_dt
                , clm_thru_dt
                , current_bene_mbi_id
                , match_pmt_amt
                , match_role
            order by
                  case when match_efctv_dt is null then 0 else 1 end
                , match_efctv_dt
                , case when file_date is null then 0 else 1 end
                , file_date
                , cur_clm_uniq_id
          ) as role_seq
    from matching_input

)

/*
    cancellations left over once the originals are taken, with the number of equal-amount
    adjustments that strictly precede each one
*/
, leftover_cancellations as (

    select
          cancellations.match_version_key
        , cancellations.role_seq - cancellations.originals_at_amount as leftover_seq
        , count(adjustments.match_version_key) as eligible_adjustments
    from match_sequences as cancellations
        left join match_sequences as adjustments
            on cancellations.clm_blg_prvdr_oscar_num = adjustments.clm_blg_prvdr_oscar_num
            and cancellations.clm_from_dt = adjustments.clm_from_dt
            and cancellations.clm_thru_dt = adjustments.clm_thru_dt
            and cancellations.current_bene_mbi_id = adjustments.current_bene_mbi_id
            and cancellations.match_pmt_amt = adjustments.match_pmt_amt
            and adjustments.match_role = 'adjustment'
            and case
                when adjustments.match_efctv_dt is not null
                    and cancellations.match_efctv_dt is not null
                    and adjustments.match_efctv_dt <> cancellations.match_efctv_dt
                    then adjustments.match_efctv_dt < cancellations.match_efctv_dt
                else coalesce(adjustments.file_date < cancellations.file_date, false)
              end
    where cancellations.match_role = 'cancellation'
        and cancellations.role_seq > cancellations.originals_at_amount
    group by
          cancellations.match_version_key
        , cancellations.role_seq - cancellations.originals_at_amount

)

, leftover_running as (

    select
          match_sequences.*
        , leftover_cancellations.leftover_seq
        , leftover_cancellations.eligible_adjustments - leftover_cancellations.leftover_seq
            as leftover_slack
    from match_sequences
        left join leftover_cancellations
            on match_sequences.match_version_key = leftover_cancellations.match_version_key

)

, leftover_taken as (

    select
          *
        , leftover_seq + case
            when min(leftover_slack) over (
                partition by
                  clm_blg_prvdr_oscar_num
                , clm_from_dt
                , clm_thru_dt
                , current_bene_mbi_id
                , match_pmt_amt
                    , case when leftover_seq is null then 0 else 1 end
                order by leftover_seq
                rows between unbounded preceding and current row
            ) < 0
            then min(leftover_slack) over (
                partition by
                  clm_blg_prvdr_oscar_num
                , clm_from_dt
                , clm_thru_dt
                , current_bene_mbi_id
                , match_pmt_amt
                    , case when leftover_seq is null then 0 else 1 end
                order by leftover_seq
                rows between unbounded preceding and current row
            )
            else 0
          end as adjustments_taken
    from leftover_running

)

, cancellation_matching as (

    select
          *
        , coalesce(max(adjustments_taken) over (
            partition by
                  clm_blg_prvdr_oscar_num
                , clm_from_dt
                , clm_thru_dt
                , current_bene_mbi_id
                , match_pmt_amt
          ), 0) as adjustments_cancelled
    from leftover_taken

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
            when match_role = 'original' and role_seq > cancellations_at_amount then 1
            when match_role = 'adjustment' and role_seq > adjustments_cancelled then 1
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
