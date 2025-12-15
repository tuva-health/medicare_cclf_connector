with sort_adjusted_claims as (

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
        , row_num
    from {{ ref('int_physician_claim_adr') }}

)

/*
    sum the adjusted line amounts

    (CCLF docs ref: 5.3 Calculating Beneficiary-Level Expenditures)
*/
, line_totals as (

    select
          clm_cntl_num
        , clm_line_num
        , current_bene_mbi_id
        , sum(clm_line_cvrd_pd_amt) as sum_clm_line_cvrd_pd_amt
        , sum(clm_line_alowd_chrg_amt) as sum_clm_line_alowd_chrg_amt
    from sort_adjusted_claims
    group by
        clm_cntl_num
      , clm_line_num
      , current_bene_mbi_id

)

/*
    apply final adjustment logic by selecting latest version of claim,
    removing any remaining claims with a canceled status, and adding line totals.
*/
, filter_claims as (

    select
          sort_adjusted_claims.cur_clm_uniq_id
        , sort_adjusted_claims.clm_line_num
        , sort_adjusted_claims.current_bene_mbi_id
        , sort_adjusted_claims.clm_from_dt
        , sort_adjusted_claims.clm_thru_dt
        , sort_adjusted_claims.clm_pos_cd
        , sort_adjusted_claims.clm_line_from_dt
        , sort_adjusted_claims.clm_line_thru_dt
        , sort_adjusted_claims.clm_line_hcpcs_cd
        , line_totals.sum_clm_line_cvrd_pd_amt as clm_line_cvrd_pd_amt
        , sort_adjusted_claims.clm_rndrg_prvdr_tax_num
        , sort_adjusted_claims.rndrg_prvdr_npi_num
        , sort_adjusted_claims.clm_adjsmt_type_cd
        , sort_adjusted_claims.clm_efctv_dt
        , sort_adjusted_claims.clm_cntl_num
        , line_totals.sum_clm_line_alowd_chrg_amt as clm_line_alowd_chrg_amt
        , sort_adjusted_claims.clm_line_srvc_unit_qty
        , sort_adjusted_claims.clm_prvdr_spclty_cd
        , sort_adjusted_claims.clm_type_cd
        , sort_adjusted_claims.hcpcs_1_mdfr_cd
        , sort_adjusted_claims.hcpcs_2_mdfr_cd
        , sort_adjusted_claims.hcpcs_3_mdfr_cd
        , sort_adjusted_claims.hcpcs_4_mdfr_cd
        , sort_adjusted_claims.hcpcs_5_mdfr_cd
        , sort_adjusted_claims.clm_dgns_1_cd
        , sort_adjusted_claims.clm_dgns_2_cd
        , sort_adjusted_claims.clm_dgns_3_cd
        , sort_adjusted_claims.clm_dgns_4_cd
        , sort_adjusted_claims.clm_dgns_5_cd
        , sort_adjusted_claims.clm_dgns_6_cd
        , sort_adjusted_claims.clm_dgns_7_cd
        , sort_adjusted_claims.clm_dgns_8_cd
        , sort_adjusted_claims.dgns_prcdr_icd_ind
        , sort_adjusted_claims.clm_dgns_9_cd
        , sort_adjusted_claims.clm_dgns_10_cd
        , sort_adjusted_claims.clm_dgns_11_cd
        , sort_adjusted_claims.clm_dgns_12_cd
        , sort_adjusted_claims.file_name
        , sort_adjusted_claims.file_date
    from sort_adjusted_claims
        left join line_totals
            on sort_adjusted_claims.clm_cntl_num = line_totals.clm_cntl_num
            and sort_adjusted_claims.clm_line_num = line_totals.clm_line_num
            and sort_adjusted_claims.current_bene_mbi_id = line_totals.current_bene_mbi_id
    where sort_adjusted_claims.row_num = 1
    and sort_adjusted_claims.clm_adjsmt_type_cd <> '1'

)

/*
    remove claim lines where claim ID+line number not unique
    even after adjustments have been applied
*/
, claim_dupes as (

    select cur_clm_uniq_id, clm_line_num
    from filter_claims
    group by cur_clm_uniq_id, clm_line_num
    having count(*) > 1

)

, remove_dupes as (

  select filter_claims.*
    from filter_claims
        left join claim_dupes
            on filter_claims.cur_clm_uniq_id = claim_dupes.cur_clm_uniq_id
            and filter_claims.clm_line_num = claim_dupes.clm_line_num
    where claim_dupes.cur_clm_uniq_id is null

)

, mapping as (

    select
          cur_clm_uniq_id as claim_id
        , clm_line_num as claim_line_number
        , cast('professional' as {{ dbt.type_string() }} ) as claim_type
        , current_bene_mbi_id as person_id
        , current_bene_mbi_id as member_id
        , cast('medicare' as {{ dbt.type_string() }} ) as payer
        , cast('medicare' as {{ dbt.type_string() }} ) as {{ the_tuva_project.quote_column('plan') }}
        , case
            when clm_from_dt in ('1000-01-01', '9999-12-31') then null
            else clm_from_dt
          end as claim_start_date
        , case
            when clm_thru_dt in ('1000-01-01', '9999-12-31') then null
            else clm_thru_dt
          end as claim_end_date
        , case
            when clm_line_from_dt in ('1000-01-01', '9999-12-31') then null
            else clm_line_from_dt
          end as claim_line_start_date
        , case
            when clm_line_thru_dt in ('1000-01-01', '9999-12-31') then null
            else clm_line_thru_dt
          end as claim_line_end_date
        , cast(null as date) as admission_date
        , cast(null as date) as discharge_date
        , cast(null as {{ dbt.type_string() }} ) as admit_source_code
        , cast(null as {{ dbt.type_string() }} ) as admit_type_code
        , cast(null as {{ dbt.type_string() }} ) as discharge_disposition_code
        , clm_pos_cd as place_of_service_code
        , cast(null as {{ dbt.type_string() }} ) as bill_type_code
        , cast(null as {{ dbt.type_string() }} ) as ms_drg_code
        , cast(null as {{ dbt.type_string() }} ) as apr_drg_code
        , cast(null as {{ dbt.type_string() }} ) as revenue_center_code
        , clm_line_srvc_unit_qty as service_unit_quantity
        , clm_prvdr_spclty_cd as claim_provider_specialty_code
        , clm_line_hcpcs_cd as hcpcs_code
        , hcpcs_1_mdfr_cd as hcpcs_modifier_1
        , hcpcs_2_mdfr_cd as hcpcs_modifier_2
        , hcpcs_3_mdfr_cd as hcpcs_modifier_3
        , hcpcs_4_mdfr_cd as hcpcs_modifier_4
        , hcpcs_5_mdfr_cd as hcpcs_modifier_5
        , clm_type_cd as clm_type_cd
        , rndrg_prvdr_npi_num as rendering_npi
        , clm_rndrg_prvdr_tax_num as rendering_tin
        , cast(null as {{ dbt.type_string() }} ) as billing_npi
        , cast(null as {{ dbt.type_string() }} ) as billing_tin
        , cast(null as {{ dbt.type_string() }} ) as facility_npi
        , case
            when clm_efctv_dt in ('1000-01-01', '9999-12-31') then null
            else clm_efctv_dt
          end as paid_date
        , clm_line_cvrd_pd_amt as paid_amount
        , clm_line_alowd_chrg_amt as allowed_amount
        , clm_line_alowd_chrg_amt as charge_amount
        , cast(null as {{ dbt.type_string() }} ) as coinsurance_amount
        , cast(null as {{ dbt.type_string() }} ) as copayment_amount
        , cast(null as {{ dbt.type_string() }} ) as deductible_amount
        , cast(null as {{ dbt.type_string() }} ) as total_cost_amount
        , case
            when cast(dgns_prcdr_icd_ind as {{ dbt.type_string() }} ) = '0' then 'icd-10-cm'
            when cast(dgns_prcdr_icd_ind as {{ dbt.type_string() }} ) = '9' then 'icd-9-cm'
            else cast(dgns_prcdr_icd_ind as {{ dbt.type_string() }} )
          end as diagnosis_code_type
        , clm_dgns_1_cd as diagnosis_code_1
        , clm_dgns_2_cd as diagnosis_code_2
        , clm_dgns_3_cd as diagnosis_code_3
        , clm_dgns_4_cd as diagnosis_code_4
        , clm_dgns_5_cd as diagnosis_code_5
        , clm_dgns_6_cd as diagnosis_code_6
        , clm_dgns_7_cd as diagnosis_code_7
        , clm_dgns_8_cd as diagnosis_code_8
        , clm_dgns_9_cd as diagnosis_code_9
        , clm_dgns_10_cd as diagnosis_code_10
        , clm_dgns_11_cd as diagnosis_code_11
        , clm_dgns_12_cd as diagnosis_code_12
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_code_13
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_code_14
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_code_15
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_code_16
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_code_17
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_code_18
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_code_19
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_code_20
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_code_21
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_code_22
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_code_23
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_code_24
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_code_25
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_1
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_2
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_3
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_4
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_5
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_6
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_7
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_8
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_9
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_10
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_11
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_12
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_13
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_14
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_15
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_16
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_17
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_18
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_19
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_20
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_21
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_22
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_23
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_24
        , cast(null as {{ dbt.type_string() }} ) as diagnosis_poa_25
        , cast(null as {{ dbt.type_string() }} ) as procedure_code_type
        , cast(null as {{ dbt.type_string() }} ) as procedure_code_1
        , cast(null as {{ dbt.type_string() }} ) as procedure_code_2
        , cast(null as {{ dbt.type_string() }} ) as procedure_code_3
        , cast(null as {{ dbt.type_string() }} ) as procedure_code_4
        , cast(null as {{ dbt.type_string() }} ) as procedure_code_5
        , cast(null as {{ dbt.type_string() }} ) as procedure_code_6
        , cast(null as {{ dbt.type_string() }} ) as procedure_code_7
        , cast(null as {{ dbt.type_string() }} ) as procedure_code_8
        , cast(null as {{ dbt.type_string() }} ) as procedure_code_9
        , cast(null as {{ dbt.type_string() }} ) as procedure_code_10
        , cast(null as {{ dbt.type_string() }} ) as procedure_code_11
        , cast(null as {{ dbt.type_string() }} ) as procedure_code_12
        , cast(null as {{ dbt.type_string() }} ) as procedure_code_13
        , cast(null as {{ dbt.type_string() }} ) as procedure_code_14
        , cast(null as {{ dbt.type_string() }} ) as procedure_code_15
        , cast(null as {{ dbt.type_string() }} ) as procedure_code_16
        , cast(null as {{ dbt.type_string() }} ) as procedure_code_17
        , cast(null as {{ dbt.type_string() }} ) as procedure_code_18
        , cast(null as {{ dbt.type_string() }} ) as procedure_code_19
        , cast(null as {{ dbt.type_string() }} ) as procedure_code_20
        , cast(null as {{ dbt.type_string() }} ) as procedure_code_21
        , cast(null as {{ dbt.type_string() }} ) as procedure_code_22
        , cast(null as {{ dbt.type_string() }} ) as procedure_code_23
        , cast(null as {{ dbt.type_string() }} ) as procedure_code_24
        , cast(null as {{ dbt.type_string() }} ) as procedure_code_25
        , cast(null as date) as procedure_date_1
        , cast(null as date) as procedure_date_2
        , cast(null as date) as procedure_date_3
        , cast(null as date) as procedure_date_4
        , cast(null as date) as procedure_date_5
        , cast(null as date) as procedure_date_6
        , cast(null as date) as procedure_date_7
        , cast(null as date) as procedure_date_8
        , cast(null as date) as procedure_date_9
        , cast(null as date) as procedure_date_10
        , cast(null as date) as procedure_date_11
        , cast(null as date) as procedure_date_12
        , cast(null as date) as procedure_date_13
        , cast(null as date) as procedure_date_14
        , cast(null as date) as procedure_date_15
        , cast(null as date) as procedure_date_16
        , cast(null as date) as procedure_date_17
        , cast(null as date) as procedure_date_18
        , cast(null as date) as procedure_date_19
        , cast(null as date) as procedure_date_20
        , cast(null as date) as procedure_date_21
        , cast(null as date) as procedure_date_22
        , cast(null as date) as procedure_date_23
        , cast(null as date) as procedure_date_24
        , cast(null as date) as procedure_date_25
        , 1 as in_network_flag
        , cast('medicare cclf' as {{ dbt.type_string() }} ) as data_source
        , file_date
        , file_name
        , file_date as ingest_datetime
    from remove_dupes

)

, add_data_types as (

    select
          cast(claim_id as {{ dbt.type_string() }} ) as claim_id
        , cast(claim_line_number as integer) as claim_line_number
        , cast(claim_type as {{ dbt.type_string() }} ) as claim_type
        , cast(person_id as {{ dbt.type_string() }} ) as person_id
        , cast(member_id as {{ dbt.type_string() }} ) as member_id
        , cast(payer as {{ dbt.type_string() }} ) as payer
        , cast({{ the_tuva_project.quote_column('plan') }} as {{ dbt.type_string() }} ) as {{ the_tuva_project.quote_column('plan') }}
        , {{ try_to_cast_date('claim_start_date', 'YYYY-MM-DD') }} as claim_start_date
        , {{ try_to_cast_date('claim_end_date', 'YYYY-MM-DD') }} as claim_end_date
        , {{ try_to_cast_date('claim_line_start_date', 'YYYY-MM-DD') }} as claim_line_start_date
        , {{ try_to_cast_date('claim_line_end_date', 'YYYY-MM-DD') }} as claim_line_end_date
        , {{ try_to_cast_date('admission_date', 'YYYY-MM-DD') }} as admission_date
        , {{ try_to_cast_date('discharge_date', 'YYYY-MM-DD') }} as discharge_date
        , cast(admit_source_code as {{ dbt.type_string() }} ) as admit_source_code
        , cast(admit_type_code as {{ dbt.type_string() }} ) as admit_type_code
        , cast(discharge_disposition_code as {{ dbt.type_string() }} ) as discharge_disposition_code
        , cast(place_of_service_code as {{ dbt.type_string() }} ) as place_of_service_code
        , cast(bill_type_code as {{ dbt.type_string() }} ) as bill_type_code
        , cast(ms_drg_code as {{ dbt.type_string() }} ) as ms_drg_code
        , cast(apr_drg_code as {{ dbt.type_string() }} ) as apr_drg_code
        , cast(revenue_center_code as {{ dbt.type_string() }} ) as revenue_center_code
        , {{ cast_numeric('service_unit_quantity') }} as service_unit_quantity
        , cast(claim_provider_specialty_code as {{ dbt.type_string() }}) as claim_provider_specialty_code
        , cast(hcpcs_code as {{ dbt.type_string() }} ) as hcpcs_code
        , cast(hcpcs_modifier_1 as {{ dbt.type_string() }} ) as hcpcs_modifier_1
        , cast(hcpcs_modifier_2 as {{ dbt.type_string() }} ) as hcpcs_modifier_2
        , cast(hcpcs_modifier_3 as {{ dbt.type_string() }} ) as hcpcs_modifier_3
        , cast(hcpcs_modifier_4 as {{ dbt.type_string() }} ) as hcpcs_modifier_4
        , cast(hcpcs_modifier_5 as {{ dbt.type_string() }} ) as hcpcs_modifier_5
        , cast(clm_type_cd as {{ dbt.type_string() }} ) as clm_type_cd
        , cast(rendering_npi as {{ dbt.type_string() }} ) as rendering_npi
        , cast(rendering_tin as {{ dbt.type_string() }} ) as rendering_tin
        , cast(billing_npi as {{ dbt.type_string() }} ) as billing_npi
        , cast(billing_tin as {{ dbt.type_string() }} ) as billing_tin
        , cast(facility_npi as {{ dbt.type_string() }} ) as facility_npi
        , {{ try_to_cast_date('paid_date', 'YYYY-MM-DD') }} as paid_date
        , {{ cast_numeric('paid_amount') }} as paid_amount
        , {{ cast_numeric('allowed_amount') }} as allowed_amount
        , {{ cast_numeric('charge_amount') }} as charge_amount
        , {{ cast_numeric('coinsurance_amount') }} as coinsurance_amount
        , {{ cast_numeric('copayment_amount') }} as copayment_amount
        , {{ cast_numeric('deductible_amount') }} as deductible_amount
        , {{ cast_numeric('total_cost_amount') }} as total_cost_amount
        , cast(diagnosis_code_type as {{ dbt.type_string() }} ) as diagnosis_code_type
        , cast(diagnosis_code_1 as {{ dbt.type_string() }} ) as diagnosis_code_1
        , cast(diagnosis_code_2 as {{ dbt.type_string() }} ) as diagnosis_code_2
        , cast(diagnosis_code_3 as {{ dbt.type_string() }} ) as diagnosis_code_3
        , cast(diagnosis_code_4 as {{ dbt.type_string() }} ) as diagnosis_code_4
        , cast(diagnosis_code_5 as {{ dbt.type_string() }} ) as diagnosis_code_5
        , cast(diagnosis_code_6 as {{ dbt.type_string() }} ) as diagnosis_code_6
        , cast(diagnosis_code_7 as {{ dbt.type_string() }} ) as diagnosis_code_7
        , cast(diagnosis_code_8 as {{ dbt.type_string() }} ) as diagnosis_code_8
        , cast(diagnosis_code_9 as {{ dbt.type_string() }} ) as diagnosis_code_9
        , cast(diagnosis_code_10 as {{ dbt.type_string() }} ) as diagnosis_code_10
        , cast(diagnosis_code_11 as {{ dbt.type_string() }} ) as diagnosis_code_11
        , cast(diagnosis_code_12 as {{ dbt.type_string() }} ) as diagnosis_code_12
        , cast(diagnosis_code_13 as {{ dbt.type_string() }} ) as diagnosis_code_13
        , cast(diagnosis_code_14 as {{ dbt.type_string() }} ) as diagnosis_code_14
        , cast(diagnosis_code_15 as {{ dbt.type_string() }} ) as diagnosis_code_15
        , cast(diagnosis_code_16 as {{ dbt.type_string() }} ) as diagnosis_code_16
        , cast(diagnosis_code_17 as {{ dbt.type_string() }} ) as diagnosis_code_17
        , cast(diagnosis_code_18 as {{ dbt.type_string() }} ) as diagnosis_code_18
        , cast(diagnosis_code_19 as {{ dbt.type_string() }} ) as diagnosis_code_19
        , cast(diagnosis_code_20 as {{ dbt.type_string() }} ) as diagnosis_code_20
        , cast(diagnosis_code_21 as {{ dbt.type_string() }} ) as diagnosis_code_21
        , cast(diagnosis_code_22 as {{ dbt.type_string() }} ) as diagnosis_code_22
        , cast(diagnosis_code_23 as {{ dbt.type_string() }} ) as diagnosis_code_23
        , cast(diagnosis_code_24 as {{ dbt.type_string() }} ) as diagnosis_code_24
        , cast(diagnosis_code_25 as {{ dbt.type_string() }} ) as diagnosis_code_25
        , cast(diagnosis_poa_1 as {{ dbt.type_string() }} ) as diagnosis_poa_1
        , cast(diagnosis_poa_2 as {{ dbt.type_string() }} ) as diagnosis_poa_2
        , cast(diagnosis_poa_3 as {{ dbt.type_string() }} ) as diagnosis_poa_3
        , cast(diagnosis_poa_4 as {{ dbt.type_string() }} ) as diagnosis_poa_4
        , cast(diagnosis_poa_5 as {{ dbt.type_string() }} ) as diagnosis_poa_5
        , cast(diagnosis_poa_6 as {{ dbt.type_string() }} ) as diagnosis_poa_6
        , cast(diagnosis_poa_7 as {{ dbt.type_string() }} ) as diagnosis_poa_7
        , cast(diagnosis_poa_8 as {{ dbt.type_string() }} ) as diagnosis_poa_8
        , cast(diagnosis_poa_9 as {{ dbt.type_string() }} ) as diagnosis_poa_9
        , cast(diagnosis_poa_10 as {{ dbt.type_string() }} ) as diagnosis_poa_10
        , cast(diagnosis_poa_11 as {{ dbt.type_string() }} ) as diagnosis_poa_11
        , cast(diagnosis_poa_12 as {{ dbt.type_string() }} ) as diagnosis_poa_12
        , cast(diagnosis_poa_13 as {{ dbt.type_string() }} ) as diagnosis_poa_13
        , cast(diagnosis_poa_14 as {{ dbt.type_string() }} ) as diagnosis_poa_14
        , cast(diagnosis_poa_15 as {{ dbt.type_string() }} ) as diagnosis_poa_15
        , cast(diagnosis_poa_16 as {{ dbt.type_string() }} ) as diagnosis_poa_16
        , cast(diagnosis_poa_17 as {{ dbt.type_string() }} ) as diagnosis_poa_17
        , cast(diagnosis_poa_18 as {{ dbt.type_string() }} ) as diagnosis_poa_18
        , cast(diagnosis_poa_19 as {{ dbt.type_string() }} ) as diagnosis_poa_19
        , cast(diagnosis_poa_20 as {{ dbt.type_string() }} ) as diagnosis_poa_20
        , cast(diagnosis_poa_21 as {{ dbt.type_string() }} ) as diagnosis_poa_21
        , cast(diagnosis_poa_22 as {{ dbt.type_string() }} ) as diagnosis_poa_22
        , cast(diagnosis_poa_23 as {{ dbt.type_string() }} ) as diagnosis_poa_23
        , cast(diagnosis_poa_24 as {{ dbt.type_string() }} ) as diagnosis_poa_24
        , cast(diagnosis_poa_25 as {{ dbt.type_string() }} ) as diagnosis_poa_25
        , cast(procedure_code_type as {{ dbt.type_string() }} ) as procedure_code_type
        , cast(procedure_code_1 as {{ dbt.type_string() }} ) as procedure_code_1
        , cast(procedure_code_2 as {{ dbt.type_string() }} ) as procedure_code_2
        , cast(procedure_code_3 as {{ dbt.type_string() }} ) as procedure_code_3
        , cast(procedure_code_4 as {{ dbt.type_string() }} ) as procedure_code_4
        , cast(procedure_code_5 as {{ dbt.type_string() }} ) as procedure_code_5
        , cast(procedure_code_6 as {{ dbt.type_string() }} ) as procedure_code_6
        , cast(procedure_code_7 as {{ dbt.type_string() }} ) as procedure_code_7
        , cast(procedure_code_8 as {{ dbt.type_string() }} ) as procedure_code_8
        , cast(procedure_code_9 as {{ dbt.type_string() }} ) as procedure_code_9
        , cast(procedure_code_10 as {{ dbt.type_string() }} ) as procedure_code_10
        , cast(procedure_code_11 as {{ dbt.type_string() }} ) as procedure_code_11
        , cast(procedure_code_12 as {{ dbt.type_string() }} ) as procedure_code_12
        , cast(procedure_code_13 as {{ dbt.type_string() }} ) as procedure_code_13
        , cast(procedure_code_14 as {{ dbt.type_string() }} ) as procedure_code_14
        , cast(procedure_code_15 as {{ dbt.type_string() }} ) as procedure_code_15
        , cast(procedure_code_16 as {{ dbt.type_string() }} ) as procedure_code_16
        , cast(procedure_code_17 as {{ dbt.type_string() }} ) as procedure_code_17
        , cast(procedure_code_18 as {{ dbt.type_string() }} ) as procedure_code_18
        , cast(procedure_code_19 as {{ dbt.type_string() }} ) as procedure_code_19
        , cast(procedure_code_20 as {{ dbt.type_string() }} ) as procedure_code_20
        , cast(procedure_code_21 as {{ dbt.type_string() }} ) as procedure_code_21
        , cast(procedure_code_22 as {{ dbt.type_string() }} ) as procedure_code_22
        , cast(procedure_code_23 as {{ dbt.type_string() }} ) as procedure_code_23
        , cast(procedure_code_24 as {{ dbt.type_string() }} ) as procedure_code_24
        , cast(procedure_code_25 as {{ dbt.type_string() }} ) as procedure_code_25
        , {{ try_to_cast_date('procedure_date_1', 'YYYY-MM-DD') }} as procedure_date_1
        , {{ try_to_cast_date('procedure_date_2', 'YYYY-MM-DD') }} as procedure_date_2
        , {{ try_to_cast_date('procedure_date_3', 'YYYY-MM-DD') }} as procedure_date_3
        , {{ try_to_cast_date('procedure_date_4', 'YYYY-MM-DD') }} as procedure_date_4
        , {{ try_to_cast_date('procedure_date_5', 'YYYY-MM-DD') }} as procedure_date_5
        , {{ try_to_cast_date('procedure_date_6', 'YYYY-MM-DD') }} as procedure_date_6
        , {{ try_to_cast_date('procedure_date_7', 'YYYY-MM-DD') }} as procedure_date_7
        , {{ try_to_cast_date('procedure_date_8', 'YYYY-MM-DD') }} as procedure_date_8
        , {{ try_to_cast_date('procedure_date_9', 'YYYY-MM-DD') }} as procedure_date_9
        , {{ try_to_cast_date('procedure_date_10', 'YYYY-MM-DD') }} as procedure_date_10
        , {{ try_to_cast_date('procedure_date_11', 'YYYY-MM-DD') }} as procedure_date_11
        , {{ try_to_cast_date('procedure_date_12', 'YYYY-MM-DD') }} as procedure_date_12
        , {{ try_to_cast_date('procedure_date_13', 'YYYY-MM-DD') }} as procedure_date_13
        , {{ try_to_cast_date('procedure_date_14', 'YYYY-MM-DD') }} as procedure_date_14
        , {{ try_to_cast_date('procedure_date_15', 'YYYY-MM-DD') }} as procedure_date_15
        , {{ try_to_cast_date('procedure_date_16', 'YYYY-MM-DD') }} as procedure_date_16
        , {{ try_to_cast_date('procedure_date_17', 'YYYY-MM-DD') }} as procedure_date_17
        , {{ try_to_cast_date('procedure_date_18', 'YYYY-MM-DD') }} as procedure_date_18
        , {{ try_to_cast_date('procedure_date_19', 'YYYY-MM-DD') }} as procedure_date_19
        , {{ try_to_cast_date('procedure_date_20', 'YYYY-MM-DD') }} as procedure_date_20
        , {{ try_to_cast_date('procedure_date_21', 'YYYY-MM-DD') }} as procedure_date_21
        , {{ try_to_cast_date('procedure_date_22', 'YYYY-MM-DD') }} as procedure_date_22
        , {{ try_to_cast_date('procedure_date_23', 'YYYY-MM-DD') }} as procedure_date_23
        , {{ try_to_cast_date('procedure_date_24', 'YYYY-MM-DD') }} as procedure_date_24
        , {{ try_to_cast_date('procedure_date_25', 'YYYY-MM-DD') }} as procedure_date_25
        , cast(in_network_flag as integer) as in_network_flag
        , cast(data_source as {{ dbt.type_string() }} ) as data_source
        , cast(file_name as {{ dbt.type_string() }} ) as file_name
        , cast(ingest_datetime as {{ dbt.type_string() }} ) as ingest_datetime
    from mapping

)

select
      claim_id
    , claim_line_number
    , claim_type
    , person_id
    , member_id
    , payer
    , {{ the_tuva_project.quote_column('plan') }}
    , claim_start_date
    , claim_end_date
    , claim_line_start_date
    , claim_line_end_date
    , admission_date
    , discharge_date
    , admit_source_code
    , admit_type_code
    , discharge_disposition_code
    , place_of_service_code
    , bill_type_code
    , ms_drg_code
    , apr_drg_code
    , revenue_center_code
    , service_unit_quantity
    , claim_provider_specialty_code
    , hcpcs_code
    , hcpcs_modifier_1
    , hcpcs_modifier_2
    , hcpcs_modifier_3
    , hcpcs_modifier_4
    , hcpcs_modifier_5
    , clm_type_cd
    , rendering_npi
    , rendering_tin
    , billing_npi
    , billing_tin
    , facility_npi
    , paid_date
    , paid_amount
    , allowed_amount
    , charge_amount
    , coinsurance_amount
    , copayment_amount
    , deductible_amount
    , total_cost_amount
    , diagnosis_code_type
    , diagnosis_code_1
    , diagnosis_code_2
    , diagnosis_code_3
    , diagnosis_code_4
    , diagnosis_code_5
    , diagnosis_code_6
    , diagnosis_code_7
    , diagnosis_code_8
    , diagnosis_code_9
    , diagnosis_code_10
    , diagnosis_code_11
    , diagnosis_code_12
    , diagnosis_code_13
    , diagnosis_code_14
    , diagnosis_code_15
    , diagnosis_code_16
    , diagnosis_code_17
    , diagnosis_code_18
    , diagnosis_code_19
    , diagnosis_code_20
    , diagnosis_code_21
    , diagnosis_code_22
    , diagnosis_code_23
    , diagnosis_code_24
    , diagnosis_code_25
    , diagnosis_poa_1
    , diagnosis_poa_2
    , diagnosis_poa_3
    , diagnosis_poa_4
    , diagnosis_poa_5
    , diagnosis_poa_6
    , diagnosis_poa_7
    , diagnosis_poa_8
    , diagnosis_poa_9
    , diagnosis_poa_10
    , diagnosis_poa_11
    , diagnosis_poa_12
    , diagnosis_poa_13
    , diagnosis_poa_14
    , diagnosis_poa_15
    , diagnosis_poa_16
    , diagnosis_poa_17
    , diagnosis_poa_18
    , diagnosis_poa_19
    , diagnosis_poa_20
    , diagnosis_poa_21
    , diagnosis_poa_22
    , diagnosis_poa_23
    , diagnosis_poa_24
    , diagnosis_poa_25
    , procedure_code_type
    , procedure_code_1
    , procedure_code_2
    , procedure_code_3
    , procedure_code_4
    , procedure_code_5
    , procedure_code_6
    , procedure_code_7
    , procedure_code_8
    , procedure_code_9
    , procedure_code_10
    , procedure_code_11
    , procedure_code_12
    , procedure_code_13
    , procedure_code_14
    , procedure_code_15
    , procedure_code_16
    , procedure_code_17
    , procedure_code_18
    , procedure_code_19
    , procedure_code_20
    , procedure_code_21
    , procedure_code_22
    , procedure_code_23
    , procedure_code_24
    , procedure_code_25
    , procedure_date_1
    , procedure_date_2
    , procedure_date_3
    , procedure_date_4
    , procedure_date_5
    , procedure_date_6
    , procedure_date_7
    , procedure_date_8
    , procedure_date_9
    , procedure_date_10
    , procedure_date_11
    , procedure_date_12
    , procedure_date_13
    , procedure_date_14
    , procedure_date_15
    , procedure_date_16
    , procedure_date_17
    , procedure_date_18
    , procedure_date_19
    , procedure_date_20
    , procedure_date_21
    , procedure_date_22
    , procedure_date_23
    , procedure_date_24
    , procedure_date_25
    , in_network_flag
    , data_source
    , file_name
    , ingest_datetime
from add_data_types