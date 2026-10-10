with staged_data as (

    select
          cast(cur_clm_uniq_id as {{ dbt.type_string() }}) as cur_clm_uniq_id
        , cast(bene_mbi_id as {{ dbt.type_string() }}) as bene_mbi_id
        , cast(bene_hic_num as {{ dbt.type_string() }}) as bene_hic_num
        , cast(clm_line_ndc_cd as {{ dbt.type_string() }}) as clm_line_ndc_cd
        , cast(clm_type_cd as {{ dbt.type_string() }}) as clm_type_cd
        , cast(clm_line_from_dt as {{ dbt.type_string() }}) as clm_line_from_dt
        , cast(prvdr_srvc_id_qlfyr_cd as {{ dbt.type_string() }}) as prvdr_srvc_id_qlfyr_cd
        , cast(clm_srvc_prvdr_gnrc_id_num as {{ dbt.type_string() }}) as clm_srvc_prvdr_gnrc_id_num
        , cast(clm_dspnsng_stus_cd as {{ dbt.type_string() }}) as clm_dspnsng_stus_cd
        , cast(clm_daw_prod_slctn_cd as {{ dbt.type_string() }}) as clm_daw_prod_slctn_cd
        , cast(clm_line_srvc_unit_qty as {{ dbt.type_string() }}) as clm_line_srvc_unit_qty
        , cast(clm_line_days_suply_qty as {{ dbt.type_string() }}) as clm_line_days_suply_qty
        , cast(prvdr_prsbng_id_qlfyr_cd as {{ dbt.type_string() }}) as prvdr_prsbng_id_qlfyr_cd
        , cast(clm_prsbng_prvdr_gnrc_id_num as {{ dbt.type_string() }}) as clm_prsbng_prvdr_gnrc_id_num
        , cast(clm_line_bene_pmt_amt as {{ dbt.type_string() }}) as clm_line_bene_pmt_amt
        , cast(clm_adjsmt_type_cd as {{ dbt.type_string() }}) as clm_adjsmt_type_cd
        , cast(clm_efctv_dt as {{ dbt.type_string() }}) as clm_efctv_dt
        , cast(clm_idr_ld_dt as {{ dbt.type_string() }}) as clm_idr_ld_dt
        , cast(clm_line_rx_srvc_rfrnc_num as {{ dbt.type_string() }}) as clm_line_rx_srvc_rfrnc_num
        , cast(clm_line_rx_fill_num as {{ dbt.type_string() }}) as clm_line_rx_fill_num
        , cast(clm_phrmcy_srvc_type_cd as {{ dbt.type_string() }}) as clm_phrmcy_srvc_type_cd
        , cast(current_bene_mbi_id as {{ dbt.type_string() }}) as current_bene_mbi_id
        , file_name
        , file_date
    from {{ ref('int_partd_claims_normalized') }}

)

/*
    dedupe full rows that may appear in multiple files
    source fields not mapped or used for adjustment logic are commented out
*/
, add_row_num as (

    select *, row_number() over (
        partition by
              cur_clm_uniq_id
            , bene_mbi_id
            , bene_hic_num
            , clm_line_ndc_cd
            /*, clm_type_cd*/
            , clm_line_from_dt
            , prvdr_srvc_id_qlfyr_cd
            , clm_srvc_prvdr_gnrc_id_num
            , clm_dspnsng_stus_cd
            /*, clm_daw_prod_slctn_cd*/
            , clm_line_srvc_unit_qty
            , clm_line_days_suply_qty
            , prvdr_prsbng_id_qlfyr_cd
            , clm_prsbng_prvdr_gnrc_id_num
            , clm_line_bene_pmt_amt
            , clm_adjsmt_type_cd
            /*, clm_efctv_dt*/
            /*, clm_idr_ld_dt*/
            , clm_line_rx_srvc_rfrnc_num
            , clm_line_rx_fill_num
            /*, clm_phrmcy_srvc_type_cd*/
        order by file_date desc
        ) as row_num
    from staged_data

)

, dedupe as (

    select
          cur_clm_uniq_id
        , bene_mbi_id
        , current_bene_mbi_id
        , bene_hic_num
        , clm_line_ndc_cd
        , clm_line_from_dt
        , prvdr_srvc_id_qlfyr_cd
        , clm_srvc_prvdr_gnrc_id_num
        , clm_dspnsng_stus_cd
        , clm_line_srvc_unit_qty
        , clm_line_days_suply_qty
        , prvdr_prsbng_id_qlfyr_cd
        , clm_prsbng_prvdr_gnrc_id_num
        , clm_line_bene_pmt_amt
        , clm_adjsmt_type_cd
        , clm_efctv_dt
        , clm_idr_ld_dt
        , clm_line_rx_srvc_rfrnc_num
        , clm_line_rx_fill_num
        , file_name
        , file_date
    from add_row_num
    where row_num = 1

)

, normalized_data as (

    select *,
        row_number() over (
            partition by
                  cur_clm_uniq_id
                , current_bene_mbi_id
                , bene_hic_num
                , clm_line_ndc_cd
                , clm_line_from_dt
                , prvdr_srvc_id_qlfyr_cd
                , clm_srvc_prvdr_gnrc_id_num
                , clm_dspnsng_stus_cd
                , clm_line_srvc_unit_qty
                , clm_line_days_suply_qty
                , prvdr_prsbng_id_qlfyr_cd
                , clm_prsbng_prvdr_gnrc_id_num
                , clm_line_bene_pmt_amt
                , clm_adjsmt_type_cd
                , clm_line_rx_srvc_rfrnc_num
                , clm_line_rx_fill_num
            order by file_date desc
        ) as normalized_row_num
    from dedupe

)

/*
    apply adjustment logic by grouping part D claims by their natural keys (CCLF IP v43 5.1.2):
     - CLM_LINE_FROM_DT
     - PRVDR_SRVC_ID_QLFYR_CD
     - CLM_SRVC_PRVDR_GNRC_ID_NUM
     - CLM_DSPNSNG_STUS_CD
     - CLM_LINE_RX_SRVC_RFRNC_NUM
     - CLM_LINE_RX_FILL_NUM

    Part D files hold only final action claims, so when monthly files are combined the
    most recent claim in a related set is its final action claim and the earlier ones are
    ignored (IP 3.3). row_num = 1 marks the most recent claim, ordered by:
     1. the delivering file's date, latest first, a missing file date last: a later delivery
        supersedes an earlier one;
     2. CLM_EFCTV_DT, latest first. A missing date (1000-01-01 / 9999-12-31, IP 3.6, cast
        to null) ranks last: a claim with no known date is never taken as more recent than
        one with a date. Most Part D rows carry 1000-01-01;
     3. CLM_IDR_LD_DT, latest first, missing last for the same reason;
     4. CLM_ADJSMT_TYPE_CD: adjustment (2), then cancellation (1), then original (0). An
        original is always processed before its cancellation and adjustment (IP 5.2.1).
        When a cancellation and an adjustment tie on every date, the cancellation is the
        one paired with the earlier version and the adjustment replaces it, so the
        adjustment is final (the same rule as Part A, IP 5.2.1);
     5. CUR_CLM_UNIQ_ID, latest first. Claim IDs carry no ordering meaning; this key only
        orders claims that tie on every key above, and keeps that pick stable.

    Each date key, the file date included, has its own is-null sort key in front of it, so
    nulls rank last on every warehouse: DuckDB and Snowflake put nulls at opposite ends of
    a descending sort by default, which would otherwise pick a different winner per
    warehouse.

    final filtering (row_num = 1, dropping a winning cancellation) takes place in the
    dedupe model
*/
, sort_adjusted_claims as (

    select
          cur_clm_uniq_id
        , bene_mbi_id
        , current_bene_mbi_id
        , bene_hic_num
        , clm_line_ndc_cd
        , clm_line_from_dt
        , prvdr_srvc_id_qlfyr_cd
        , clm_srvc_prvdr_gnrc_id_num
        , clm_dspnsng_stus_cd
        , clm_line_srvc_unit_qty
        , clm_line_days_suply_qty
        , prvdr_prsbng_id_qlfyr_cd
        , clm_prsbng_prvdr_gnrc_id_num
        , clm_line_bene_pmt_amt
        , clm_adjsmt_type_cd
        , clm_line_rx_srvc_rfrnc_num
        , clm_line_rx_fill_num
        , file_name
        , file_date
        , row_number() over (
            partition by
                  clm_line_from_dt
                , prvdr_srvc_id_qlfyr_cd
                , clm_srvc_prvdr_gnrc_id_num
                , clm_dspnsng_stus_cd
                , clm_line_rx_srvc_rfrnc_num
                , clm_line_rx_fill_num
            order by
                  case when file_date is null then 1 else 0 end
                , file_date desc
                , case when {{ try_to_cast_date('clm_efctv_dt') }} is null then 1 else 0 end
                , {{ try_to_cast_date('clm_efctv_dt') }} desc
                , case when {{ try_to_cast_date('clm_idr_ld_dt') }} is null then 1 else 0 end
                , {{ try_to_cast_date('clm_idr_ld_dt') }} desc
                , case clm_adjsmt_type_cd
                    when '2' then 1
                    when '1' then 2
                    when '0' then 3
                    else 4
                  end
                , cur_clm_uniq_id desc
        ) as row_num
    from normalized_data
    where normalized_row_num = 1

)

select
      cur_clm_uniq_id
    , bene_mbi_id
    , current_bene_mbi_id
    , bene_hic_num
    , clm_line_ndc_cd
    , clm_line_from_dt
    , prvdr_srvc_id_qlfyr_cd
    , clm_srvc_prvdr_gnrc_id_num
    , clm_dspnsng_stus_cd
    , clm_line_srvc_unit_qty
    , clm_line_days_suply_qty
    , prvdr_prsbng_id_qlfyr_cd
    , clm_prsbng_prvdr_gnrc_id_num
    , clm_line_bene_pmt_amt
    , clm_adjsmt_type_cd
    , clm_line_rx_srvc_rfrnc_num
    , clm_line_rx_fill_num
    , file_name
    , file_date
    , row_num
from sort_adjusted_claims
