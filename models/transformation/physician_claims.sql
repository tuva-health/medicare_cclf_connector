{{
    config( materialized='ephemeral' )
}}

select
    cast(h.cur_clm_uniq_id as varchar) as claim_id
    ,cast(h.clm_line_num as int) as claim_line_number
    ,cast(h.bene_mbi_id as varchar) as patient_id
    ,cast(h.clm_from_dt as date) as claim_start_date
    ,cast(h.clm_thru_dt as date) as claim_end_date
    ,cast(NULL as date) as admission_date
    ,cast(NULL as date) as discharge_date
    ,cast(h.clm_line_from_dt as date) as claim_line_start_date
    ,cast(h.clm_line_thru_dt as date) as claim_line_end_date
    ,cast('P' as varchar) as claim_type
    ,cast(NULL as varchar) as bill_type_code
    ,cast(h.clm_pos_cd as varchar) as place_of_service_code
    ,cast(NULL as varchar) as admit_source_code
    ,cast(NULL as varchar) as admit_type_code
    ,cast(NULL as varchar) as discharge_disposition_code
    ,cast(NULL as varchar) as ms_drg
    ,cast(NULL as varchar) as revenue_center_code
    ,cast(left(clm_line_srvc_unit_qty,charindex('.',clm_line_srvc_unit_qty)-1) as int) as service_unit_quantity
    ,cast(h.clm_line_hcpcs_cd as varchar) as hcpcs_code
    ,cast(h.hcpcs_1_mdfr_cd as varchar) as hcpcs_modifier_1
    ,cast(h.hcpcs_2_mdfr_cd as varchar) as hcpcs_modifier_2
    ,cast(h.hcpcs_3_mdfr_cd as varchar) as hcpcs_modifier_3
    ,cast(h.hcpcs_4_mdfr_cd as varchar) as hcpcs_modifier_4
    ,cast(h.hcpcs_5_mdfr_cd as varchar) as hcpcs_modifier_5
    ,cast(h.rndrg_prvdr_npi_num as varchar) as physician_npi
    ,cast(NULL as varchar) as facility_npi
    ,cast(NULL as date) as paid_date
    ,cast(clm_line_cvrd_pd_amt as float) as paid_amount
    ,cast(clm_line_alowd_chrg_amt as float) as charge_amount
    ,cast(isnull(clm_adjsmt_type_cd, 'O') as varchar) as adjustment_type_code
    ,cast(h.clm_dgns_1_cd as varchar) as diagnosis_code_1
    ,cast(h.clm_dgns_2_cd as varchar) as diagnosis_code_2
    ,cast(h.clm_dgns_3_cd as varchar) as diagnosis_code_3
    ,cast(h.clm_dgns_4_cd as varchar) as diagnosis_code_4
    ,cast(h.clm_dgns_5_cd as varchar) as diagnosis_code_5
    ,cast(h.clm_dgns_6_cd as varchar) as diagnosis_code_6
    ,cast(h.clm_dgns_7_cd as varchar) as diagnosis_code_7
    ,cast(h.clm_dgns_8_cd as varchar) as diagnosis_code_8
    ,cast(h.clm_dgns_9_cd as varchar) as diagnosis_code_9
    ,cast(h.clm_dgns_10_cd as varchar) as diagnosis_code_10
    ,cast(h.clm_dgns_11_cd as varchar) as diagnosis_code_11
    ,cast(h.clm_dgns_12_cd as varchar) as diagnosis_code_12
    ,cast(NULL as varchar) as diagnosis_code_13
    ,cast(NULL as varchar) as diagnosis_code_14
    ,cast(NULL as varchar) as diagnosis_code_15
    ,cast(NULL as varchar) as diagnosis_code_16
    ,cast(NULL as varchar) as diagnosis_code_17
    ,cast(NULL as varchar) as diagnosis_code_18
    ,cast(NULL as varchar) as diagnosis_code_19
    ,cast(NULL as varchar) as diagnosis_code_20
    ,cast(NULL as varchar) as diagnosis_code_21
    ,cast(NULL as varchar) as diagnosis_code_22
    ,cast(NULL as varchar) as diagnosis_code_23
    ,cast(NULL as varchar) as diagnosis_code_24
    ,cast(NULL as varchar) as diagnosis_code_25
    ,cast(NULL as varchar) as diagnosis_poa_1
    ,cast(NULL as varchar) as diagnosis_poa_2
    ,cast(NULL as varchar) as diagnosis_poa_3
    ,cast(NULL as varchar) as diagnosis_poa_4
    ,cast(NULL as varchar) as diagnosis_poa_5
    ,cast(NULL as varchar) as diagnosis_poa_6
    ,cast(NULL as varchar) as diagnosis_poa_7
    ,cast(NULL as varchar) as diagnosis_poa_8
    ,cast(NULL as varchar) as diagnosis_poa_9
    ,cast(NULL as varchar) as diagnosis_poa_10
    ,cast(NULL as varchar) as diagnosis_poa_11
    ,cast(NULL as varchar) as diagnosis_poa_12
    ,cast(NULL as varchar) as diagnosis_poa_13
    ,cast(NULL as varchar) as diagnosis_poa_14
    ,cast(NULL as varchar) as diagnosis_poa_15
    ,cast(NULL as varchar) as diagnosis_poa_16
    ,cast(NULL as varchar) as diagnosis_poa_17
    ,cast(NULL as varchar) as diagnosis_poa_18
    ,cast(NULL as varchar) as diagnosis_poa_19
    ,cast(NULL as varchar) as diagnosis_poa_20
    ,cast(NULL as varchar) as diagnosis_poa_21
    ,cast(NULL as varchar) as diagnosis_poa_22
    ,cast(NULL as varchar) as diagnosis_poa_23
    ,cast(NULL as varchar) as diagnosis_poa_24
    ,cast(NULL as varchar) as diagnosis_poa_25
    ,cast(h.dgns_prcdr_icd_ind as varchar) as diagnosis_code_type
    ,cast(NULL as varchar) as procedure_code_type
    ,cast(NULL as varchar) as procedure_code_1
    ,cast(NULL as varchar) as procedure_code_2
    ,cast(NULL as varchar) as procedure_code_3
    ,cast(NULL as varchar) as procedure_code_4
    ,cast(NULL as varchar) as procedure_code_5
    ,cast(NULL as varchar) as procedure_code_6
    ,cast(NULL as varchar) as procedure_code_7
    ,cast(NULL as varchar) as procedure_code_8
    ,cast(NULL as varchar) as procedure_code_9
    ,cast(NULL as varchar) as procedure_code_10
    ,cast(NULL as varchar) as procedure_code_11
    ,cast(NULL as varchar) as procedure_code_12
    ,cast(NULL as varchar) as procedure_code_13
    ,cast(NULL as varchar) as procedure_code_14
    ,cast(NULL as varchar) as procedure_code_15
    ,cast(NULL as varchar) as procedure_code_16
    ,cast(NULL as varchar) as procedure_code_17
    ,cast(NULL as varchar) as procedure_code_18
    ,cast(NULL as varchar) as procedure_code_19
    ,cast(NULL as varchar) as procedure_code_20
    ,cast(NULL as varchar) as procedure_code_21
    ,cast(NULL as varchar) as procedure_code_22
    ,cast(NULL as varchar) as procedure_code_23
    ,cast(NULL as varchar) as procedure_code_24
    ,cast(NULL as varchar) as procedure_code_25
    ,cast(NULL as date) as procedure_date_1
    ,cast(NULL as date) as procedure_date_2
    ,cast(NULL as date) as procedure_date_3
    ,cast(NULL as date) as procedure_date_4
    ,cast(NULL as date) as procedure_date_5
    ,cast(NULL as date) as procedure_date_6
    ,cast(NULL as date) as procedure_date_7
    ,cast(NULL as date) as procedure_date_8
    ,cast(NULL as date) as procedure_date_9
    ,cast(NULL as date) as procedure_date_10
    ,cast(NULL as date) as procedure_date_11
    ,cast(NULL as date) as procedure_date_12
    ,cast(NULL as date) as procedure_date_13
    ,cast(NULL as date) as procedure_date_14
    ,cast(NULL as date) as procedure_date_15
    ,cast(NULL as date) as procedure_date_16
    ,cast(NULL as date) as procedure_date_17
    ,cast(NULL as date) as procedure_date_18
    ,cast(NULL as date) as procedure_date_19
    ,cast(NULL as date) as procedure_date_20
    ,cast(NULL as date) as procedure_date_21
    ,cast(NULL as date) as procedure_date_22
    ,cast(NULL as date) as procedure_date_23
    ,cast(NULL as date) as procedure_date_24
    ,cast(NULL as date) as procedure_date_25
from {{ var('partb_physicians')}} h

