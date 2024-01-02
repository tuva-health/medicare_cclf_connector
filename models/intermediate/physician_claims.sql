select
      cast(cur_clm_uniq_id as {{ dbt.type_string() }} ) as claim_id
    , cast(clm_line_num as integer) as claim_line_number
    , 'professional' as claim_type
    , cast(bene_mbi_id as {{ dbt.type_string() }} ) as patient_id
    , cast(bene_mbi_id as {{ dbt.type_string() }} ) as member_id
    , 'medicare' as payer
    , 'medicare' as plan
    , {{ try_to_cast_date('clm_from_dt', 'YYYY-MM-DD') }} as claim_start_date
    , {{ try_to_cast_date('clm_thru_dt', 'YYYY-MM-DD') }} as claim_end_date
    , {{ try_to_cast_date('clm_line_from_dt', 'YYYY-MM-DD') }} as claim_line_start_date
    , {{ try_to_cast_date('clm_line_thru_dt', 'YYYY-MM-DD') }} as claim_line_end_date
    , cast(NULL as date) as admission_date
    , cast(NULL as date) as discharge_date
    , cast(NULL as {{ dbt.type_string() }} ) as admit_source_code
    , cast(NULL as {{ dbt.type_string() }} ) as admit_type_code
    , cast(NULL as {{ dbt.type_string() }} ) as discharge_disposition_code
    , cast(clm_pos_cd as {{ dbt.type_string() }} ) as place_of_service_code
    , cast(NULL as {{ dbt.type_string() }} ) as bill_type_code
    , cast(NULL as {{ dbt.type_string() }} ) as ms_drg_code
    , cast(NULL as {{ dbt.type_string() }} ) as apr_drg_code
    , cast(NULL as {{ dbt.type_string() }} ) as revenue_center_code
    , cast(clm_line_srvc_unit_qty as integer) as service_unit_quantity
    , cast(clm_line_hcpcs_cd as {{ dbt.type_string() }} ) as hcpcs_code
    , cast(hcpcs_1_mdfr_cd as {{ dbt.type_string() }} ) as hcpcs_modifier_1
    , cast(hcpcs_2_mdfr_cd as {{ dbt.type_string() }} ) as hcpcs_modifier_2
    , cast(hcpcs_3_mdfr_cd as {{ dbt.type_string() }} ) as hcpcs_modifier_3
    , cast(hcpcs_4_mdfr_cd as {{ dbt.type_string() }} ) as hcpcs_modifier_4
    , cast(hcpcs_5_mdfr_cd as {{ dbt.type_string() }} ) as hcpcs_modifier_5
    , cast(rndrg_prvdr_npi_num as {{ dbt.type_string() }} ) as rendering_npi
    , cast(NULL as {{ dbt.type_string() }} ) as billing_npi
    , cast(NULL as {{ dbt.type_string() }} ) as facility_npi
    , cast(NULL as date) as paid_date
    , {{ cast_numeric('clm_line_cvrd_pd_amt') }} as paid_amount
    , {{ cast_numeric('clm_line_alowd_chrg_amt') }} as allowed_amount
    , {{ cast_numeric('clm_line_alowd_chrg_amt') }} as charge_amount
    , {{ cast_numeric('NULL') }} as coinsurance_amount
    , {{ cast_numeric('NULL') }} as copayment_amount
    , {{ cast_numeric('NULL') }} as deductible_amount
    , {{ cast_numeric('NULL') }} as total_cost_amount
    , case
        when cast(dgns_prcdr_icd_ind as {{ dbt.type_string() }} ) = '0' then 'icd-10-pcs'
        when cast(dgns_prcdr_icd_ind as {{ dbt.type_string() }} ) = '9' then 'icd-9-pcs'
        else cast(dgns_prcdr_icd_ind as {{ dbt.type_string() }} ) end as diagnosis_code_type
    , cast(clm_dgns_1_cd as {{ dbt.type_string() }} ) as diagnosis_code_1
    , cast(clm_dgns_2_cd as {{ dbt.type_string() }} ) as diagnosis_code_2
    , cast(clm_dgns_3_cd as {{ dbt.type_string() }} ) as diagnosis_code_3
    , cast(clm_dgns_4_cd as {{ dbt.type_string() }} ) as diagnosis_code_4
    , cast(clm_dgns_5_cd as {{ dbt.type_string() }} ) as diagnosis_code_5
    , cast(clm_dgns_6_cd as {{ dbt.type_string() }} ) as diagnosis_code_6
    , cast(clm_dgns_7_cd as {{ dbt.type_string() }} ) as diagnosis_code_7
    , cast(clm_dgns_8_cd as {{ dbt.type_string() }} ) as diagnosis_code_8
    , cast(clm_dgns_9_cd as {{ dbt.type_string() }} ) as diagnosis_code_9
    , cast(clm_dgns_10_cd as {{ dbt.type_string() }} ) as diagnosis_code_10
    , cast(clm_dgns_11_cd as {{ dbt.type_string() }} ) as diagnosis_code_11
    , cast(clm_dgns_12_cd as {{ dbt.type_string() }} ) as diagnosis_code_12
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_13
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_14
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_15
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_16
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_17
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_18
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_19
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_20
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_21
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_22
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_23
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_24
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_code_25
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_1
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_2
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_3
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_4
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_5
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_6
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_7
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_8
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_9
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_10
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_11
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_12
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_13
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_14
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_15
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_16
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_17
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_18
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_19
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_20
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_21
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_22
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_23
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_24
    , cast(NULL as {{ dbt.type_string() }} ) as diagnosis_poa_25
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_type
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_1
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_2
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_3
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_4
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_5
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_6
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_7
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_8
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_9
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_10
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_11
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_12
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_13
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_14
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_15
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_16
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_17
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_18
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_19
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_20
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_21
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_22
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_23
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_24
    , cast(NULL as {{ dbt.type_string() }} ) as procedure_code_25
    , cast(NULL as date) as procedure_date_1
    , cast(NULL as date) as procedure_date_2
    , cast(NULL as date) as procedure_date_3
    , cast(NULL as date) as procedure_date_4
    , cast(NULL as date) as procedure_date_5
    , cast(NULL as date) as procedure_date_6
    , cast(NULL as date) as procedure_date_7
    , cast(NULL as date) as procedure_date_8
    , cast(NULL as date) as procedure_date_9
    , cast(NULL as date) as procedure_date_10
    , cast(NULL as date) as procedure_date_11
    , cast(NULL as date) as procedure_date_12
    , cast(NULL as date) as procedure_date_13
    , cast(NULL as date) as procedure_date_14
    , cast(NULL as date) as procedure_date_15
    , cast(NULL as date) as procedure_date_16
    , cast(NULL as date) as procedure_date_17
    , cast(NULL as date) as procedure_date_18
    , cast(NULL as date) as procedure_date_19
    , cast(NULL as date) as procedure_date_20
    , cast(NULL as date) as procedure_date_21
    , cast(NULL as date) as procedure_date_22
    , cast(NULL as date) as procedure_date_23
    , cast(NULL as date) as procedure_date_24
    , cast(NULL as date) as procedure_date_25
    , 'medicare cclf' as data_source
from {{ source('medicare_cclf','partb_physicians')}}
