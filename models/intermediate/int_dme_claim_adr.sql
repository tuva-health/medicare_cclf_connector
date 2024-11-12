with staged_data as (

    select
          cur_clm_uniq_id
        , clm_line_num
        , bene_mbi_id
        , bene_hic_num
        , clm_type_cd
        , clm_from_dt
        , clm_thru_dt
        , clm_fed_type_srvc_cd
        , clm_pos_cd
        , clm_line_from_dt
        , clm_line_thru_dt
        , clm_line_hcpcs_cd
        , clm_line_cvrd_pd_amt
        , clm_prmry_pyr_cd
        , payto_prvdr_npi_num
        , ordrg_prvdr_npi_num
        , clm_carr_pmt_dnl_cd
        , clm_prcsg_ind_cd
        , clm_adjsmt_type_cd
        , clm_efctv_dt
        , clm_idr_ld_dt
        , clm_cntl_num
        , bene_eqtbl_bic_hicn_num
        , clm_line_alowd_chrg_amt
        , clm_disp_cd
        , file_name as file_name
        , file_row_number
        , ingest_datetime as ingest_date
    from {{ ref('stg_cclf6') }}

)

, beneficiary_xref as (

  select * from {{ ref('int_beneficiary_xref_deduped') }}

)

, parse_file_date as (

     select *
          , CAST(SUBSTRING(file_name, CHARINDEX('D', file_name) + 1, 6) as date )  as file_date 
     from staged_data

)

/* 
    dedupe full rows that may appear in multiple files
    
*/
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
            , clm_fed_type_srvc_cd
            , clm_pos_cd
            , clm_line_from_dt
            , clm_line_thru_dt
            , clm_line_hcpcs_cd
            , clm_line_cvrd_pd_amt
            , clm_prmry_pyr_cd
            , payto_prvdr_npi_num
            , ordrg_prvdr_npi_num
            , clm_carr_pmt_dnl_cd
            , clm_prcsg_ind_cd
            , clm_adjsmt_type_cd
            , clm_efctv_dt
            , clm_idr_ld_dt
            , clm_cntl_num
            , bene_eqtbl_bic_hicn_num
            , clm_line_alowd_chrg_amt
            , clm_disp_cd
        order by file_date desc
        ) as row_num
    from parse_file_date

)

/* 
    source fields not mapped or used for adjustment logic are commented out 

    removing "#" from claim id and claim control number before they are used in partition
*/
, dedupe as (

    select
          replace(cur_clm_uniq_id,'#','') as cur_clm_uniq_id
        , clm_line_num
        , bene_mbi_id
        /*, bene_hic_num*/
        /*, clm_type_cd*/
        , clm_from_dt
        , clm_thru_dt
        /*, clm_fed_type_srvc_cd*/
        , clm_pos_cd
        , clm_line_from_dt
        , clm_line_thru_dt
        , clm_line_hcpcs_cd
        , clm_line_cvrd_pd_amt
        /*, clm_prmry_pyr_cd*/
        , payto_prvdr_npi_num
        , ordrg_prvdr_npi_num
        /*, clm_carr_pmt_dnl_cd*/
        /*, clm_prcsg_ind_cd*/
        , clm_adjsmt_type_cd
        , clm_efctv_dt
        /*, clm_idr_ld_dt*/
        , replace(clm_cntl_num,'#','') as clm_cntl_num
        /*, bene_eqtbl_bic_hicn_num*/
        , clm_line_alowd_chrg_amt
        /*, clm_disp_cd*/
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
        , dedupe.payto_prvdr_npi_num
        , dedupe.ordrg_prvdr_npi_num
        , dedupe.clm_adjsmt_type_cd
        , dedupe.clm_efctv_dt
        , dedupe.clm_cntl_num
        , dedupe.clm_line_alowd_chrg_amt
        , dedupe.file_name
        , dedupe.file_date
    from dedupe
        left join beneficiary_xref
            on dedupe.bene_mbi_id = beneficiary_xref.prvs_num


)

/*
    1) apply adjustment logic by grouping part B DME claims by their natural keys:
     - CLM_CNTL_NUM
     - Most Recent MBI
     - CLM_LINE_NUM (not listed in CCLF docs, but used to prevent line detail loss)

    2) sort grouped claims by the latest CLM_EFCTV_DT and CUR_CLM_UNIQ_ID since CLM_ADJSMT_TYPE_CD
    is not used consistently to indciate the latest final version of an adjusted claim.

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
            when clm_adjsmt_type_cd = '1' then cast(clm_line_cvrd_pd_amt as {{dbt.type_numeric()}} ) * -1
            else cast(clm_line_cvrd_pd_amt as {{dbt.type_numeric()}} )
          end as clm_line_cvrd_pd_amt
        , payto_prvdr_npi_num
        , ordrg_prvdr_npi_num
        , clm_adjsmt_type_cd
        , clm_efctv_dt
        , clm_cntl_num
        , case
            when clm_adjsmt_type_cd = '1' then cast(clm_line_alowd_chrg_amt as {{dbt.type_numeric()}} ) * -1
            else cast(clm_line_alowd_chrg_amt as {{dbt.type_numeric()}} )
          end as clm_line_alowd_chrg_amt
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
    , payto_prvdr_npi_num
    , ordrg_prvdr_npi_num
    , clm_adjsmt_type_cd
    , clm_efctv_dt
    , clm_cntl_num
    , clm_line_alowd_chrg_amt
    , file_name
    , file_date
    , row_num
from sort_adjusted_claims