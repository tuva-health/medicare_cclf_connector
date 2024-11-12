with sort_adjusted_claims as (

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
    from {{ ref('int_institutional_claim_adr') }}

)

, diagnosis_pivot as (

    select * from {{ ref('int_diagnosis_pivot') }}

)

, procedure_pivot as (

    select * from {{ ref('int_procedure_pivot') }}

)

, revenue_center as (

    select * from {{ ref('int_revenue_center_deduped') }}

)

/*
    sum the adjusted header amounts

    (CCLF docs ref: 5.3 Calculating Beneficiary-Level Expenditures)
*/
, header_totals as (

    select
          clm_blg_prvdr_oscar_num
        , clm_from_dt
        , clm_thru_dt
        , current_bene_mbi_id
        , sum(clm_pmt_amt) as sum_clm_pmt_amt
        , sum(clm_mdcr_instnl_tot_chrg_amt) as sum_clm_mdcr_instnl_tot_chrg_amt
    from sort_adjusted_claims
    group by
          clm_blg_prvdr_oscar_num
        , clm_from_dt
        , clm_thru_dt
        , current_bene_mbi_id

)

/*
    apply final adjustment logic by selecting latest version of claim,
    removing any remaining claims with a canceled status, and adding header totals.
*/
, filter_claims as (

    select
          sort_adjusted_claims.cur_clm_uniq_id
        , sort_adjusted_claims.current_bene_mbi_id
        , sort_adjusted_claims.clm_from_dt
        , sort_adjusted_claims.clm_thru_dt
        , sort_adjusted_claims.clm_bill_fac_type_cd
        , sort_adjusted_claims.clm_bill_clsfctn_cd
        , header_totals.sum_clm_pmt_amt as clm_pmt_amt
        , sort_adjusted_claims.bene_ptnt_stus_cd
        , sort_adjusted_claims.dgns_drg_cd
        , sort_adjusted_claims.fac_prvdr_npi_num
        , sort_adjusted_claims.atndg_prvdr_npi_num
        , sort_adjusted_claims.clm_adjsmt_type_cd
        , sort_adjusted_claims.clm_efctv_dt
        , sort_adjusted_claims.clm_admsn_type_cd
        , sort_adjusted_claims.clm_admsn_src_cd
        , sort_adjusted_claims.clm_bill_freq_cd
        , sort_adjusted_claims.dgns_prcdr_icd_ind
        , header_totals.sum_clm_mdcr_instnl_tot_chrg_amt as clm_mdcr_instnl_tot_chrg_amt
        , sort_adjusted_claims.clm_blg_prvdr_oscar_num
        , sort_adjusted_claims.file_name
        , sort_adjusted_claims.file_date
    from sort_adjusted_claims
        left join header_totals
            on sort_adjusted_claims.clm_blg_prvdr_oscar_num = header_totals.clm_blg_prvdr_oscar_num
            and sort_adjusted_claims.clm_from_dt = header_totals.clm_from_dt
            and sort_adjusted_claims.clm_thru_dt = header_totals.clm_thru_dt
            and sort_adjusted_claims.current_bene_mbi_id = header_totals.current_bene_mbi_id
    where sort_adjusted_claims.row_num = 1
    and sort_adjusted_claims.clm_adjsmt_type_cd <> '1'

)

, add_claim_line_details as (

    select
          filter_claims.cur_clm_uniq_id
        , filter_claims.current_bene_mbi_id
        , filter_claims.clm_from_dt
        , filter_claims.clm_thru_dt
        , filter_claims.clm_bill_fac_type_cd
        , filter_claims.clm_bill_clsfctn_cd
        , filter_claims.clm_pmt_amt
        , filter_claims.bene_ptnt_stus_cd
        , filter_claims.dgns_drg_cd
        , filter_claims.fac_prvdr_npi_num
        , filter_claims.atndg_prvdr_npi_num
        , filter_claims.clm_adjsmt_type_cd
        , filter_claims.clm_efctv_dt
        , filter_claims.clm_admsn_type_cd
        , filter_claims.clm_admsn_src_cd
        , filter_claims.clm_bill_freq_cd
        , filter_claims.dgns_prcdr_icd_ind
        , filter_claims.clm_mdcr_instnl_tot_chrg_amt
        , filter_claims.clm_blg_prvdr_oscar_num
        , filter_claims.file_name
        , filter_claims.file_date
        , revenue_center.clm_line_num
        , revenue_center.clm_line_from_dt
        , revenue_center.clm_line_thru_dt
        , revenue_center.clm_line_prod_rev_ctr_cd
        , revenue_center.clm_line_hcpcs_cd
        , revenue_center.clm_line_srvc_unit_qty
        , revenue_center.clm_line_cvrd_pd_amt
        , revenue_center.hcpcs_1_mdfr_cd
        , revenue_center.hcpcs_2_mdfr_cd
        , revenue_center.hcpcs_3_mdfr_cd
        , revenue_center.hcpcs_4_mdfr_cd
        , revenue_center.hcpcs_5_mdfr_cd
        , diagnosis_pivot.dgns_prcdr_icd_ind as diagnosis_icd_ind
        , diagnosis_pivot.diagnosis_code_1
        , diagnosis_pivot.diagnosis_code_2
        , diagnosis_pivot.diagnosis_code_3
        , diagnosis_pivot.diagnosis_code_4
        , diagnosis_pivot.diagnosis_code_5
        , diagnosis_pivot.diagnosis_code_6
        , diagnosis_pivot.diagnosis_code_7
        , diagnosis_pivot.diagnosis_code_8
        , diagnosis_pivot.diagnosis_code_9
        , diagnosis_pivot.diagnosis_code_10
        , diagnosis_pivot.diagnosis_code_11
        , diagnosis_pivot.diagnosis_code_12
        , diagnosis_pivot.diagnosis_code_13
        , diagnosis_pivot.diagnosis_code_14
        , diagnosis_pivot.diagnosis_code_15
        , diagnosis_pivot.diagnosis_code_16
        , diagnosis_pivot.diagnosis_code_17
        , diagnosis_pivot.diagnosis_code_18
        , diagnosis_pivot.diagnosis_code_19
        , diagnosis_pivot.diagnosis_code_20
        , diagnosis_pivot.diagnosis_code_21
        , diagnosis_pivot.diagnosis_code_22
        , diagnosis_pivot.diagnosis_code_23
        , diagnosis_pivot.diagnosis_code_24
        , diagnosis_pivot.diagnosis_code_25
        , diagnosis_pivot.diagnosis_poa_1
        , diagnosis_pivot.diagnosis_poa_2
        , diagnosis_pivot.diagnosis_poa_3
        , diagnosis_pivot.diagnosis_poa_4
        , diagnosis_pivot.diagnosis_poa_5
        , diagnosis_pivot.diagnosis_poa_6
        , diagnosis_pivot.diagnosis_poa_7
        , diagnosis_pivot.diagnosis_poa_8
        , diagnosis_pivot.diagnosis_poa_9
        , diagnosis_pivot.diagnosis_poa_10
        , diagnosis_pivot.diagnosis_poa_11
        , diagnosis_pivot.diagnosis_poa_12
        , diagnosis_pivot.diagnosis_poa_13
        , diagnosis_pivot.diagnosis_poa_14
        , diagnosis_pivot.diagnosis_poa_15
        , diagnosis_pivot.diagnosis_poa_16
        , diagnosis_pivot.diagnosis_poa_17
        , diagnosis_pivot.diagnosis_poa_18
        , diagnosis_pivot.diagnosis_poa_19
        , diagnosis_pivot.diagnosis_poa_20
        , diagnosis_pivot.diagnosis_poa_21
        , diagnosis_pivot.diagnosis_poa_22
        , diagnosis_pivot.diagnosis_poa_23
        , diagnosis_pivot.diagnosis_poa_24
        , diagnosis_pivot.diagnosis_poa_25
        , procedure_pivot.dgns_prcdr_icd_ind as procedure_icd_ind
        , procedure_pivot.procedure_code_1
        , procedure_pivot.procedure_code_2
        , procedure_pivot.procedure_code_3
        , procedure_pivot.procedure_code_4
        , procedure_pivot.procedure_code_5
        , procedure_pivot.procedure_code_6
        , procedure_pivot.procedure_code_7
        , procedure_pivot.procedure_code_8
        , procedure_pivot.procedure_code_9
        , procedure_pivot.procedure_code_10
        , procedure_pivot.procedure_code_11
        , procedure_pivot.procedure_code_12
        , procedure_pivot.procedure_code_13
        , procedure_pivot.procedure_code_14
        , procedure_pivot.procedure_code_15
        , procedure_pivot.procedure_code_16
        , procedure_pivot.procedure_code_17
        , procedure_pivot.procedure_code_18
        , procedure_pivot.procedure_code_19
        , procedure_pivot.procedure_code_20
        , procedure_pivot.procedure_code_21
        , procedure_pivot.procedure_code_22
        , procedure_pivot.procedure_code_23
        , procedure_pivot.procedure_code_24
        , procedure_pivot.procedure_code_25
        , procedure_pivot.procedure_date_1
        , procedure_pivot.procedure_date_2
        , procedure_pivot.procedure_date_3
        , procedure_pivot.procedure_date_4
        , procedure_pivot.procedure_date_5
        , procedure_pivot.procedure_date_6
        , procedure_pivot.procedure_date_7
        , procedure_pivot.procedure_date_8
        , procedure_pivot.procedure_date_9
        , procedure_pivot.procedure_date_10
        , procedure_pivot.procedure_date_11
        , procedure_pivot.procedure_date_12
        , procedure_pivot.procedure_date_13
        , procedure_pivot.procedure_date_14
        , procedure_pivot.procedure_date_15
        , procedure_pivot.procedure_date_16
        , procedure_pivot.procedure_date_17
        , procedure_pivot.procedure_date_18
        , procedure_pivot.procedure_date_19
        , procedure_pivot.procedure_date_20
        , procedure_pivot.procedure_date_21
        , procedure_pivot.procedure_date_22
        , procedure_pivot.procedure_date_23
        , procedure_pivot.procedure_date_24
        , procedure_pivot.procedure_date_25
    from filter_claims
        left join revenue_center
            on filter_claims.cur_clm_uniq_id = revenue_center.cur_clm_uniq_id
            /* adding part a natural keys to prevent duplicate lines  */
            and filter_claims.clm_blg_prvdr_oscar_num = prvdr_oscar_num
            and filter_claims.clm_from_dt = revenue_center.clm_from_dt
            and filter_claims.clm_thru_dt = revenue_center.clm_thru_dt
            and filter_claims.current_bene_mbi_id = revenue_center.current_bene_mbi_id
        left join diagnosis_pivot
            on filter_claims.cur_clm_uniq_id = diagnosis_pivot.cur_clm_uniq_id
            and filter_claims.current_bene_mbi_id = diagnosis_pivot.bene_mbi_id
        left join procedure_pivot
            on filter_claims.cur_clm_uniq_id = procedure_pivot.cur_clm_uniq_id
            and filter_claims.current_bene_mbi_id = procedure_pivot.bene_mbi_id

)

/*
    removing claim lines where claim ID+line number not unique
    even after adjustments have been applied
*/
, claim_dupes as (

    select cur_clm_uniq_id, clm_line_num
    from add_claim_line_details
    group by cur_clm_uniq_id, clm_line_num
    having count(*) > 1

)

, remove_dupes as (

    select add_claim_line_details.*
    from add_claim_line_details
        left join claim_dupes
            on add_claim_line_details.cur_clm_uniq_id = claim_dupes.cur_clm_uniq_id
            and add_claim_line_details.clm_line_num = claim_dupes.clm_line_num
    where claim_dupes.cur_clm_uniq_id is null

)

/***

    Determine claim paid amount following guidance from CCLF docs:

    3.5 Part A Header Expenditures vs Part A Revenue Center Expenditures
    Both the Part A Header file (CCLF1) and the Part A Revenue Center file (CCLF2) contain a
    payment related field, entitled CLM_PMT_AMT and CLM_LINE_CVRD_PD_AMT, respectively.
    The revenue center payment amounts should only be relied on if they sum to the header level
    payment amount. If the revenue center level payment amounts do not sum to the header level
    payment amount, then the revenue center level payment amounts should be ignored.

***/

/* get claim payment from original claim header grain */
, claim_header_total as (

    select
          cur_clm_uniq_id
        , {{ cast_numeric('clm_pmt_amt') }} as clm_pmt_amt
    from filter_claims

)

/* sum claim line totals from deduped claim lines */
, claim_line_total as (

    select
          cur_clm_uniq_id
        , sum({{ cast_numeric('clm_line_cvrd_pd_amt') }}) as sum_clm_line_cvrd_pd_amt
    from remove_dupes
    group by cur_clm_uniq_id

)

/* create flag to determine if claim line payments should be used */
, check_payment_totals as (

    select
          claim_header_total.cur_clm_uniq_id
        , claim_header_total.clm_pmt_amt
        , claim_line_total.sum_clm_line_cvrd_pd_amt
        , case
            when claim_header_total.clm_pmt_amt = claim_line_total.sum_clm_line_cvrd_pd_amt then 1
            else 0
          end as use_line_payments_flag
    from claim_header_total
        left join claim_line_total
          on claim_header_total.cur_clm_uniq_id = claim_line_total.cur_clm_uniq_id

)

, mapping as (

    select
          remove_dupes.cur_clm_uniq_id as claim_id
          /* fill in line number for claims with no revenue center details */
        , coalesce(clm_line_num, 1) as claim_line_number
        , 'institutional' as claim_type
        , current_bene_mbi_id as patient_id
        , current_bene_mbi_id as member_id
        , 'medicare' as payer
        , 'medicare' as plan
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
        , case
            when nullif(clm_admsn_type_cd,'~') is not null then clm_from_dt
            else null
          end as admission_date
        , case
            when nullif(clm_admsn_type_cd,'~') is not null then clm_thru_dt
            else null
          end as discharge_date
        , nullif(clm_admsn_src_cd,'~') as admit_source_code
        , nullif(clm_admsn_type_cd,'~') as admit_type_code
        , lpad(bene_ptnt_stus_cd, 2, '0') as discharge_disposition_code
        , null as place_of_service_code
        , {{ dbt.concat(
            [
                "clm_bill_fac_type_cd",
                "clm_bill_clsfctn_cd",
                "clm_bill_freq_cd"
            ]
          ) }} as bill_type_code
        , case
            when len(dgns_drg_cd) > 3 then right(dgns_drg_cd,3)
            else dgns_drg_cd
          end as ms_drg_code
        , null as apr_drg_code
        , clm_line_prod_rev_ctr_cd as revenue_center_code
        , clm_line_srvc_unit_qty as service_unit_quantity
        , clm_line_hcpcs_cd as hcpcs_code
        , hcpcs_1_mdfr_cd as hcpcs_modifier_1
        , hcpcs_2_mdfr_cd as hcpcs_modifier_2
        , hcpcs_3_mdfr_cd as hcpcs_modifier_3
        , hcpcs_4_mdfr_cd as hcpcs_modifier_4
        , hcpcs_5_mdfr_cd as hcpcs_modifier_5
        , atndg_prvdr_npi_num as rendering_npi
        , null as rendering_tin
        , null as billing_npi
        , null as billing_tin
        , fac_prvdr_npi_num as facility_npi
        , case
            when clm_efctv_dt in ('1000-01-01', '9999-12-31') then null
            else clm_efctv_dt
          end as paid_date
          /* use flag to determine if claim line payments should be used */
        , case
            when use_line_payments_flag = 1 then clm_line_cvrd_pd_amt
            when use_line_payments_flag = 0 and coalesce(clm_line_num, 1) = 1 then remove_dupes.clm_pmt_amt
            else 0
          end as paid_amount
        , null as allowed_amount
        , case
            when revenue_center_code = '0001'
            then clm_mdcr_instnl_tot_chrg_amt
            else null
          end as charge_amount
        , null as coinsurance_amount
        , null as copayment_amount
        , null as deductible_amount
        , null as total_cost_amount
        , case
            when cast(diagnosis_icd_ind as {{ dbt.type_string() }} ) = '0' then 'icd-10-cm'
            when cast(diagnosis_icd_ind as {{ dbt.type_string() }} ) = '9' then 'icd-9-cm'
            else cast(diagnosis_icd_ind as {{ dbt.type_string() }} )
          end as diagnosis_code_type
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
        , case
            when cast(procedure_icd_ind as {{ dbt.type_string() }} ) = '0' then 'icd-10-cm'
            when cast(procedure_icd_ind as {{ dbt.type_string() }} ) = '9' then 'icd-9-cm'
            else cast(procedure_icd_ind as {{ dbt.type_string() }} )
          end as procedure_code_type
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
        , 1 as in_network_flag
        , 'medicare cclf' as data_source
        , file_name
        , file_date as ingest_datetime
    from remove_dupes
        left join check_payment_totals
            on remove_dupes.cur_clm_uniq_id = check_payment_totals.cur_clm_uniq_id

)

, add_data_types as (

    select
          cast(claim_id as {{ dbt.type_string() }} ) as claim_id
        , cast(claim_line_number as integer) as claim_line_number
        , cast(claim_type as {{ dbt.type_string() }} ) as claim_type
        , cast(patient_id as {{ dbt.type_string() }} ) as patient_id
        , cast(member_id as {{ dbt.type_string() }} ) as member_id
        , cast(payer as {{ dbt.type_string() }} ) as payer
        , cast(plan as {{ dbt.type_string() }} ) as plan
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
        , cast(service_unit_quantity as float) as service_unit_quantity
        , cast(hcpcs_code as {{ dbt.type_string() }} ) as hcpcs_code
        , cast(hcpcs_modifier_1 as {{ dbt.type_string() }} ) as hcpcs_modifier_1
        , cast(hcpcs_modifier_2 as {{ dbt.type_string() }} ) as hcpcs_modifier_2
        , cast(hcpcs_modifier_3 as {{ dbt.type_string() }} ) as hcpcs_modifier_3
        , cast(hcpcs_modifier_4 as {{ dbt.type_string() }} ) as hcpcs_modifier_4
        , cast(hcpcs_modifier_5 as {{ dbt.type_string() }} ) as hcpcs_modifier_5
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
        , cast(ingest_datetime as {{ dbt.type_timestamp() }} ) as ingest_datetime
    from mapping

)

select
      claim_id
    , claim_line_number
    , claim_type
    , patient_id
    , member_id
    , payer
    , plan
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
    , hcpcs_code
    , hcpcs_modifier_1
    , hcpcs_modifier_2
    , hcpcs_modifier_3
    , hcpcs_modifier_4
    , hcpcs_modifier_5
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