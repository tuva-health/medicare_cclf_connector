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
        -- Exclude denied claims
        and not ((upper(trim(clm_prcsg_ind_cd)) not in ('A','O','S','R')) or clm_carr_pmt_dnl_cd = '0')
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
                  clm_cntl_num
                , current_bene_mbi_id
          ) as group_has_adjustment
    from normalized_data
    where normalized_row_num = 1

)

/*
    1) apply adjustment logic by grouping part B Physician claims by their natural keys:
     - CLM_CNTL_NUM
     - Most Recent MBI
     - CLM_LINE_NUM (not listed in CCLF docs, but used to prevent line detail loss)
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
            when group_has_adjustment = 1 then cast('' as {{ dbt.type_string() }})
            else cur_clm_uniq_id
          end as adjustment_key
        , row_number() over (
            partition by
                  clm_cntl_num
                , clm_line_num
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
    , adjustment_key
    , row_num
from sort_adjusted_claims
