with sort_adjusted_claims as (

    select
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
        , file_name
        , file_date
        , row_num
    from {{ ref('int_pharmacy_claim_adr') }}

)

/*
    apply final adjustment logic by selecting latest version of claim
    and removing cancelled claims

    CLM_ADJSMT_TYPE_CD:
    0 = Original Claim
    1 = Cancellation Claim
    2 = Adjustment claim
*/
, filter_claims as (

    select
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
        , file_name
        , file_date
        , row_num
    from sort_adjusted_claims
    where row_num = 1
    and clm_adjsmt_type_cd <> '1'

)

/*
    remove claim lines where claim ID+line number not unique
    even after adjustments have been applied
*/
, claim_dupes as (

    select cur_clm_uniq_id
    from filter_claims
    group by cur_clm_uniq_id
    having count(*) > 1

)

, remove_dupes as (

    select filter_claims.*
    from filter_claims
        left join claim_dupes
            on filter_claims.cur_clm_uniq_id = claim_dupes.cur_clm_uniq_id
    where claim_dupes.cur_clm_uniq_id is null

)

, mapping as (

    select
          cur_clm_uniq_id as claim_id
        , 1 as claim_line_number
        , current_bene_mbi_id as person_id
        , current_bene_mbi_id as member_id
        , cast('medicare' as {{ dbt.type_string() }} ) as payer
        , cast('medicare'as {{ dbt.type_string() }} ) as {{ the_tuva_project.quote_column('plan') }}
        , case
            when prvdr_prsbng_id_qlfyr_cd in ('1', '01')
            then clm_prsbng_prvdr_gnrc_id_num
            else null
            end as prescribing_provider_npi
        , case
            when prvdr_srvc_id_qlfyr_cd in ('1', '01')
            then clm_srvc_prvdr_gnrc_id_num
            else null
            end as dispensing_provider_npi
        , clm_line_from_dt as dispensing_date
        , clm_line_ndc_cd as ndc_code
        , clm_line_srvc_unit_qty as quantity
        , clm_line_days_suply_qty as days_supply
        , clm_line_rx_fill_num as refills
        , clm_line_from_dt as paid_date
        , clm_line_bene_pmt_amt as paid_amount
        , cast(null as {{ dbt.type_string() }} ) as allowed_amount
        , cast(null as {{ dbt.type_string() }} ) as charge_amount
        , cast(null as {{ dbt.type_string() }} ) as coinsurance_amount
        , clm_line_bene_pmt_amt as copayment_amount
        , cast(null as {{ dbt.type_string() }} ) as deductible_amount
        , 1 as in_network_flag
        , cast('medicare cclf' as {{ dbt.type_string() }} ) as data_source
        , file_name as file_name
        , file_date as file_date
        , file_date as ingest_datetime
    from remove_dupes

)

select
      claim_id
    , claim_line_number
    , person_id
    , member_id
    , payer
    , {{ the_tuva_project.quote_column('plan') }}
    , prescribing_provider_npi
    , dispensing_provider_npi
    , dispensing_date
    , ndc_code
    , quantity
    , days_supply
    , refills
    , paid_date
    , paid_amount
    , allowed_amount
    , charge_amount
    , coinsurance_amount
    , copayment_amount
    , deductible_amount
    , in_network_flag
    , data_source
    , file_name
    , file_date
    , ingest_datetime
from mapping