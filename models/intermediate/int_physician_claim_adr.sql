with staged_data as (

    select
          cast(cur_clm_uniq_id as {{ dbt.type_string() }}) as cur_clm_uniq_id
        , cast(clm_line_num as {{ dbt.type_string() }}) as clm_line_num
        , cast(bene_mbi_id as {{ dbt.type_string() }}) as bene_mbi_id
        , cast(bene_hic_num as {{ dbt.type_string() }}) as bene_hic_num
        , cast(clm_type_cd as {{ dbt.type_string() }}) as clm_type_cd
        , cast(clm_from_dt as {{ dbt.type_string() }}) as clm_from_dt
        , cast(clm_thru_dt as {{ dbt.type_string() }}) as clm_thru_dt
        , cast(rndrg_prvdr_type_cd as {{ dbt.type_string() }}) as rndrg_prvdr_type_cd
        , cast(rndrg_prvdr_fips_st_cd as {{ dbt.type_string() }}) as rndrg_prvdr_fips_st_cd
        , cast(clm_prvdr_spclty_cd as {{ dbt.type_string() }}) as clm_prvdr_spclty_cd
        , cast(clm_fed_type_srvc_cd as {{ dbt.type_string() }}) as clm_fed_type_srvc_cd
        , cast(clm_pos_cd as {{ dbt.type_string() }}) as clm_pos_cd
        , cast(clm_line_from_dt as {{ dbt.type_string() }}) as clm_line_from_dt
        , cast(clm_line_thru_dt as {{ dbt.type_string() }}) as clm_line_thru_dt
        , cast(clm_line_hcpcs_cd as {{ dbt.type_string() }}) as clm_line_hcpcs_cd
        , cast(clm_line_cvrd_pd_amt as {{ dbt.type_string() }}) as clm_line_cvrd_pd_amt
        , cast(clm_line_prmry_pyr_cd as {{ dbt.type_string() }}) as clm_line_prmry_pyr_cd
        , cast(clm_line_dgns_cd as {{ dbt.type_string() }}) as clm_line_dgns_cd
        , cast(clm_rndrg_prvdr_tax_num as {{ dbt.type_string() }}) as clm_rndrg_prvdr_tax_num
        , cast(rndrg_prvdr_npi_num as {{ dbt.type_string() }}) as rndrg_prvdr_npi_num
        , cast(clm_carr_pmt_dnl_cd as {{ dbt.type_string() }}) as clm_carr_pmt_dnl_cd
        , cast(clm_prcsg_ind_cd as {{ dbt.type_string() }}) as clm_prcsg_ind_cd
        , cast(clm_adjsmt_type_cd as {{ dbt.type_string() }}) as clm_adjsmt_type_cd
        , cast(clm_efctv_dt as {{ dbt.type_string() }}) as clm_efctv_dt
        , cast(clm_idr_ld_dt as {{ dbt.type_string() }}) as clm_idr_ld_dt
        , cast(clm_cntl_num as {{ dbt.type_string() }}) as clm_cntl_num
        , cast(bene_eqtbl_bic_hicn_num as {{ dbt.type_string() }}) as bene_eqtbl_bic_hicn_num
        , cast(clm_line_alowd_chrg_amt as {{ dbt.type_string() }}) as clm_line_alowd_chrg_amt
        , cast(clm_line_srvc_unit_qty as {{ dbt.type_string() }}) as clm_line_srvc_unit_qty
        , cast(hcpcs_1_mdfr_cd as {{ dbt.type_string() }}) as hcpcs_1_mdfr_cd
        , cast(hcpcs_2_mdfr_cd as {{ dbt.type_string() }}) as hcpcs_2_mdfr_cd
        , cast(hcpcs_3_mdfr_cd as {{ dbt.type_string() }}) as hcpcs_3_mdfr_cd
        , cast(hcpcs_4_mdfr_cd as {{ dbt.type_string() }}) as hcpcs_4_mdfr_cd
        , cast(hcpcs_5_mdfr_cd as {{ dbt.type_string() }}) as hcpcs_5_mdfr_cd
        , cast(clm_disp_cd as {{ dbt.type_string() }}) as clm_disp_cd
        , cast(clm_dgns_1_cd as {{ dbt.type_string() }}) as clm_dgns_1_cd
        , cast(clm_dgns_2_cd as {{ dbt.type_string() }}) as clm_dgns_2_cd
        , cast(clm_dgns_3_cd as {{ dbt.type_string() }}) as clm_dgns_3_cd
        , cast(clm_dgns_4_cd as {{ dbt.type_string() }}) as clm_dgns_4_cd
        , cast(clm_dgns_5_cd as {{ dbt.type_string() }}) as clm_dgns_5_cd
        , cast(clm_dgns_6_cd as {{ dbt.type_string() }}) as clm_dgns_6_cd
        , cast(clm_dgns_7_cd as {{ dbt.type_string() }}) as clm_dgns_7_cd
        , cast(clm_dgns_8_cd as {{ dbt.type_string() }}) as clm_dgns_8_cd
        , cast(dgns_prcdr_icd_ind as {{ dbt.type_string() }}) as dgns_prcdr_icd_ind
        , cast(clm_dgns_9_cd as {{ dbt.type_string() }}) as clm_dgns_9_cd
        , cast(clm_dgns_10_cd as {{ dbt.type_string() }}) as clm_dgns_10_cd
        , cast(clm_dgns_11_cd as {{ dbt.type_string() }}) as clm_dgns_11_cd
        , cast(clm_dgns_12_cd as {{ dbt.type_string() }}) as clm_dgns_12_cd
        , cast(hcpcs_betos_cd as {{ dbt.type_string() }}) as hcpcs_betos_cd
        , cast(current_bene_mbi_id as {{ dbt.type_string() }}) as current_bene_mbi_id
        , file_name
        , file_date
    from {{ ref('int_partb_physicians_normalized') }}

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
        , current_bene_mbi_id
        /*, bene_hic_num*/
        , clm_type_cd
        , clm_from_dt
        , clm_thru_dt
        /*, rndrg_prvdr_type_cd*/
        /*, rndrg_prvdr_fips_st_cd*/
        , clm_prvdr_spclty_cd
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
        /*
            Exclude denied claims (see README, "Denied claims"). Part B physician claims are
            dropped on CLM_CARR_PMT_DNL_CD and their line items on CLM_PRCSG_IND_CD. A missing
            CLM_CARR_PMT_DNL_CD is not a denial code, so it does not drop the claim.
        */
        and not ((upper(trim(clm_prcsg_ind_cd)) not in ('A','O','S','R')) or coalesce(clm_carr_pmt_dnl_cd, '') = '0')
)

/*
    dedupe re-delivered copies of the same claim version. CMS re-delivers claims across
    monthly files and later file layouts blank columns such as HIC and BETOS, so a full-row
    partition cannot collapse them. A claim version is identified by its claim ID, line,
    adjustment type and effective date; the most recently delivered copy wins.
*/
, normalized_data as (

    select *,
        row_number() over (
            partition by
                  cur_clm_uniq_id
                , clm_line_num
                , clm_adjsmt_type_cd
                , clm_efctv_dt
            order by file_date desc
        ) as normalized_row_num
    from dedupe

)

/*
    identify the final action line(s) in each related set of Part B physician claim lines.

    A related set is every claim line sharing the Part B natural key (CCLF IP v43 5.1.2),
    with the line number added so line detail is not lost:
     - CLM_CNTL_NUM
     - Most Recent MBI
     - CLM_LINE_NUM

    IP 5.2.2 and 5.2.3 define the final action claims as what remains once each cancellation
    (CLM_ADJSMT_TYPE_CD 1) is matched with an original (0) or adjustment (2) claim and every
    matched pair is removed. A related set can hold more than one final action line (two
    originals, or the IP 5.3.2 Table 6 example, where the cancellation shares its effective
    date with the surviving original and cancels the older one), and a cancellation is never
    one. Neither CLM_EFCTV_DT, CLM_ADJSMT_TYPE_CD nor CUR_CLM_UNIQ_ID identifies the final
    line: a cancellation and its replacement adjustment are usually processed together and
    share CLM_EFCTV_DT, and so can an original and its cancellation. So the related set is
    not ranked to a single winner.

    This is the Part A rule of int_institutional_claim_adr, applied to the line payment
    amount; see that model for the reasoning behind each step. In short:
     - A cancellation copies the line it cancels, so it pairs only with an original or
       adjustment of equal CLM_LINE_CVRD_PD_AMT in its related set.
     - An equal-amount original is always an eligible partner.
     - An equal-amount adjustment is eligible only if it strictly precedes the
       cancellation: an earlier CLM_EFCTV_DT, or, when the effective dates are equal or
       either is missing, an earlier file date. An adjustment with the same effective date
       and file date as the cancellation is its replacement, not its target. Adjustments do
       occur in CCLF5 and CCLF6 although the IP describes Part B as original + cancellation.
     - Cancellations take partners in age order (CLM_EFCTV_DT, then file date, then
       CUR_CLM_UNIQ_ID, missing dates first), each taking the oldest eligible original
       first, else the oldest eligible adjustment. The set-based form below reproduces
       that sequence, as in the Part A model.
     - A cancellation with no eligible partner removes nothing and is itself dropped. That
       happens when the line it cancels predates the loaded files.

    A related set made up only of originals keeps every line. Cancellation amounts are made
    negative for beneficiary-level debit/credit totals (IP 5.3.1); a final action line keeps
    its own payment and allowed amounts.
*/
, matching_input as (

    select
          *
        , {{ try_to_cast_numeric('clm_line_cvrd_pd_amt') }} as match_pmt_amt
        , case
            when clm_adjsmt_type_cd = '1' then 'cancellation'
            when clm_adjsmt_type_cd = '0' then 'original'
            else 'adjustment'
          end as match_role
        , {{ try_to_cast_date('clm_efctv_dt') }} as match_efctv_dt
        -- a claim line version is unique on these columns after normalized_data
        , cur_clm_uniq_id
            || '|' || coalesce(clm_line_num, '')
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
                  clm_cntl_num
                , clm_line_num
                , current_bene_mbi_id
                , match_pmt_amt
          ) as cancellations_at_amount
        , sum(case when match_role = 'original' then 1 else 0 end) over (
            partition by
                  clm_cntl_num
                , clm_line_num
                , current_bene_mbi_id
                , match_pmt_amt
          ) as originals_at_amount
        , row_number() over (
            partition by
                  clm_cntl_num
                , clm_line_num
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
            on cancellations.clm_cntl_num = adjustments.clm_cntl_num
            and cancellations.clm_line_num = adjustments.clm_line_num
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
                      clm_cntl_num
                    , clm_line_num
                    , current_bene_mbi_id
                    , match_pmt_amt
                    , case when leftover_seq is null then 0 else 1 end
                order by leftover_seq
                rows between unbounded preceding and current row
            ) < 0
            then min(leftover_slack) over (
                partition by
                      clm_cntl_num
                    , clm_line_num
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
                  clm_cntl_num
                , clm_line_num
                , current_bene_mbi_id
                , match_pmt_amt
          ), 0) as adjustments_cancelled
    from leftover_taken

)

, sort_adjusted_claims as (

    select
          cur_clm_uniq_id
        , clm_line_num
        , bene_mbi_id
        , current_bene_mbi_id
        , clm_from_dt
        , clm_thru_dt
        , clm_pos_cd
        , clm_line_from_dt
        , clm_line_thru_dt
        , clm_line_hcpcs_cd
        , case
            when clm_adjsmt_type_cd = '1' then {{ try_to_cast_numeric('clm_line_cvrd_pd_amt') }} * -1
            else {{ try_to_cast_numeric('clm_line_cvrd_pd_amt') }}
          end as clm_line_cvrd_pd_amt
        , clm_rndrg_prvdr_tax_num
        , rndrg_prvdr_npi_num
        , clm_adjsmt_type_cd
        , clm_efctv_dt
        , clm_cntl_num
        , case
            when clm_adjsmt_type_cd = '1' then {{ try_to_cast_numeric('clm_line_alowd_chrg_amt') }} * -1
            else {{ try_to_cast_numeric('clm_line_alowd_chrg_amt') }}
          end as clm_line_alowd_chrg_amt
        , clm_line_srvc_unit_qty
        , clm_prvdr_spclty_cd
        , hcpcs_1_mdfr_cd
        , hcpcs_2_mdfr_cd
        , hcpcs_3_mdfr_cd
        , hcpcs_4_mdfr_cd
        , hcpcs_5_mdfr_cd
        , clm_type_cd
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
        , case
            when match_role = 'original' and role_seq > cancellations_at_amount then 1
            when match_role = 'adjustment' and role_seq > adjustments_cancelled then 1
            else 0
          end as final_action_flag
    from cancellation_matching

)

select
      cur_clm_uniq_id
    , clm_line_num
    , bene_mbi_id
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
    , clm_prvdr_spclty_cd
    , clm_type_cd
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
    , final_action_flag
from sort_adjusted_claims
