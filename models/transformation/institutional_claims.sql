select
      {{ cast_string_or_varchar('h.cur_clm_uniq_id') }} as claim_id
    , cast(d.clm_line_num as integer) as claim_line_number
    , 'institutional' as claim_type
    , {{ cast_string_or_varchar('h.bene_mbi_id') }} as patient_id
    , {{ cast_string_or_varchar('NULL') }} as member_id
    , {{ try_to_cast_date('h.clm_from_dt', 'YYYY-MM-DD') }} as claim_start_date
    , {{ try_to_cast_date('h.clm_thru_dt', 'YYYY-MM-DD') }} as claim_end_date
    , cast(NULL as date) as claim_line_start_date
    , cast(NULL as date) as claim_line_end_date
    , cast(NULL as date) as admission_date
    , cast(NULL as date) as discharge_date
    , {{ cast_string_or_varchar('h.clm_admsn_src_cd') }} as admit_source_code
    , {{ cast_string_or_varchar('h.clm_admsn_type_cd') }} as admit_type_code
    , {{ cast_string_or_varchar('h.bene_ptnt_stus_cd') }} as discharge_disposition_code
    , {{ cast_string_or_varchar('NULL') }} as place_of_service_code
    , {{ cast_string_or_varchar('h.clm_bill_fac_type_cd') }}
        || {{ cast_string_or_varchar('h.clm_bill_clsfctn_cd') }}
        || {{ cast_string_or_varchar('h.clm_bill_freq_cd') }}
      as bill_type_code
    , {{ cast_string_or_varchar('h.dgns_drg_cd') }} as ms_drg_code
    ,  lpad({{ cast_string_or_varchar('d.clm_line_prod_rev_ctr_cd') }},4,'0') as revenue_center_code
    , cast(d.clm_line_srvc_unit_qty as integer) as service_unit_quantity
    , {{ cast_string_or_varchar('d.clm_line_hcpcs_cd') }} as hcpcs_code
    , {{ cast_string_or_varchar('d.hcpcs_1_mdfr_cd') }} as hcpcs_modifier_1
    , {{ cast_string_or_varchar('d.hcpcs_2_mdfr_cd') }} as hcpcs_modifier_2
    , {{ cast_string_or_varchar('d.hcpcs_3_mdfr_cd') }} as hcpcs_modifier_3
    , {{ cast_string_or_varchar('d.hcpcs_4_mdfr_cd') }} as hcpcs_modifier_4
    , {{ cast_string_or_varchar('d.hcpcs_5_mdfr_cd') }} as hcpcs_modifier_5
    , {{ cast_string_or_varchar('h.atndg_prvdr_npi_num') }} as rendering_npi
    , {{ cast_string_or_varchar('NULL') }} as billing_npi
    , {{ cast_string_or_varchar('h.fac_prvdr_npi_num') }} as facility_npi
    , cast(NULL as date) as paid_date
    , {{ cast_numeric('h.clm_pmt_amt') }} as paid_amount
    , {{ cast_numeric('NULL') }} as allowed_amount
    , {{ cast_numeric('h.clm_mdcr_instnl_tot_chrg_amt') }} as charge_amount
    , {{ cast_string_or_varchar('dx.dgns_prcdr_icd_ind') }} as diagnosis_code_type
    , {{ cast_string_or_varchar('dx.diagnosis_code_1') }} as diagnosis_code_1
    , {{ cast_string_or_varchar('dx.diagnosis_code_2') }} as diagnosis_code_2
    , {{ cast_string_or_varchar('dx.diagnosis_code_3') }} as diagnosis_code_3
    , {{ cast_string_or_varchar('dx.diagnosis_code_4') }} as diagnosis_code_4
    , {{ cast_string_or_varchar('dx.diagnosis_code_5') }} as diagnosis_code_5
    , {{ cast_string_or_varchar('dx.diagnosis_code_6') }} as diagnosis_code_6
    , {{ cast_string_or_varchar('dx.diagnosis_code_7') }} as diagnosis_code_7
    , {{ cast_string_or_varchar('dx.diagnosis_code_8') }} as diagnosis_code_8
    , {{ cast_string_or_varchar('dx.diagnosis_code_9') }} as diagnosis_code_9
    , {{ cast_string_or_varchar('dx.diagnosis_code_10') }} as diagnosis_code_10
    , {{ cast_string_or_varchar('dx.diagnosis_code_11') }} as diagnosis_code_11
    , {{ cast_string_or_varchar('dx.diagnosis_code_12') }} as diagnosis_code_12
    , {{ cast_string_or_varchar('dx.diagnosis_code_13') }} as diagnosis_code_13
    , {{ cast_string_or_varchar('dx.diagnosis_code_14') }} as diagnosis_code_14
    , {{ cast_string_or_varchar('dx.diagnosis_code_15') }} as diagnosis_code_15
    , {{ cast_string_or_varchar('dx.diagnosis_code_16') }} as diagnosis_code_16
    , {{ cast_string_or_varchar('dx.diagnosis_code_17') }} as diagnosis_code_17
    , {{ cast_string_or_varchar('dx.diagnosis_code_18') }} as diagnosis_code_18
    , {{ cast_string_or_varchar('dx.diagnosis_code_19') }} as diagnosis_code_19
    , {{ cast_string_or_varchar('dx.diagnosis_code_20') }} as diagnosis_code_20
    , {{ cast_string_or_varchar('dx.diagnosis_code_21') }} as diagnosis_code_21
    , {{ cast_string_or_varchar('dx.diagnosis_code_22') }} as diagnosis_code_22
    , {{ cast_string_or_varchar('dx.diagnosis_code_23') }} as diagnosis_code_23
    , {{ cast_string_or_varchar('dx.diagnosis_code_24') }} as diagnosis_code_24
    , {{ cast_string_or_varchar('dx.diagnosis_code_25') }} as diagnosis_code_25
    , {{ cast_string_or_varchar('dx.diagnosis_poa_1') }} as diagnosis_poa_1
    , {{ cast_string_or_varchar('dx.diagnosis_poa_2') }} as diagnosis_poa_2
    , {{ cast_string_or_varchar('dx.diagnosis_poa_3') }} as diagnosis_poa_3
    , {{ cast_string_or_varchar('dx.diagnosis_poa_4') }} as diagnosis_poa_4
    , {{ cast_string_or_varchar('dx.diagnosis_poa_5') }} as diagnosis_poa_5
    , {{ cast_string_or_varchar('dx.diagnosis_poa_6') }} as diagnosis_poa_6
    , {{ cast_string_or_varchar('dx.diagnosis_poa_7') }} as diagnosis_poa_7
    , {{ cast_string_or_varchar('dx.diagnosis_poa_8') }} as diagnosis_poa_8
    , {{ cast_string_or_varchar('dx.diagnosis_poa_9') }} as diagnosis_poa_9
    , {{ cast_string_or_varchar('dx.diagnosis_poa_10') }} as diagnosis_poa_10
    , {{ cast_string_or_varchar('dx.diagnosis_poa_11') }} as diagnosis_poa_11
    , {{ cast_string_or_varchar('dx.diagnosis_poa_12') }} as diagnosis_poa_12
    , {{ cast_string_or_varchar('dx.diagnosis_poa_13') }} as diagnosis_poa_13
    , {{ cast_string_or_varchar('dx.diagnosis_poa_14') }} as diagnosis_poa_14
    , {{ cast_string_or_varchar('dx.diagnosis_poa_15') }} as diagnosis_poa_15
    , {{ cast_string_or_varchar('dx.diagnosis_poa_16') }} as diagnosis_poa_16
    , {{ cast_string_or_varchar('dx.diagnosis_poa_17') }} as diagnosis_poa_17
    , {{ cast_string_or_varchar('dx.diagnosis_poa_18') }} as diagnosis_poa_18
    , {{ cast_string_or_varchar('dx.diagnosis_poa_19') }} as diagnosis_poa_19
    , {{ cast_string_or_varchar('dx.diagnosis_poa_20') }} as diagnosis_poa_20
    , {{ cast_string_or_varchar('dx.diagnosis_poa_21') }} as diagnosis_poa_21
    , {{ cast_string_or_varchar('dx.diagnosis_poa_22') }} as diagnosis_poa_22
    , {{ cast_string_or_varchar('dx.diagnosis_poa_23') }} as diagnosis_poa_23
    , {{ cast_string_or_varchar('dx.diagnosis_poa_24') }} as diagnosis_poa_24
    , {{ cast_string_or_varchar('dx.diagnosis_poa_25') }} as diagnosis_poa_25
    , {{ cast_string_or_varchar('px.dgns_prcdr_icd_ind') }} as procedure_code_type
    , {{ cast_string_or_varchar('px.procedure_code_1') }} as procedure_code_1
    , {{ cast_string_or_varchar('px.procedure_code_2') }} as procedure_code_2
    , {{ cast_string_or_varchar('px.procedure_code_3') }} as procedure_code_3
    , {{ cast_string_or_varchar('px.procedure_code_4') }} as procedure_code_4
    , {{ cast_string_or_varchar('px.procedure_code_5') }} as procedure_code_5
    , {{ cast_string_or_varchar('px.procedure_code_6') }} as procedure_code_6
    , {{ cast_string_or_varchar('px.procedure_code_7') }} as procedure_code_7
    , {{ cast_string_or_varchar('px.procedure_code_8') }} as procedure_code_8
    , {{ cast_string_or_varchar('px.procedure_code_9') }} as procedure_code_9
    , {{ cast_string_or_varchar('px.procedure_code_10') }} as procedure_code_10
    , {{ cast_string_or_varchar('px.procedure_code_11') }} as procedure_code_11
    , {{ cast_string_or_varchar('px.procedure_code_12') }} as procedure_code_12
    , {{ cast_string_or_varchar('px.procedure_code_13') }} as procedure_code_13
    , {{ cast_string_or_varchar('px.procedure_code_14') }} as procedure_code_14
    , {{ cast_string_or_varchar('px.procedure_code_15') }} as procedure_code_15
    , {{ cast_string_or_varchar('px.procedure_code_16') }} as procedure_code_16
    , {{ cast_string_or_varchar('px.procedure_code_17') }} as procedure_code_17
    , {{ cast_string_or_varchar('px.procedure_code_18') }} as procedure_code_18
    , {{ cast_string_or_varchar('px.procedure_code_19') }} as procedure_code_19
    , {{ cast_string_or_varchar('px.procedure_code_20') }} as procedure_code_20
    , {{ cast_string_or_varchar('px.procedure_code_21') }} as procedure_code_21
    , {{ cast_string_or_varchar('px.procedure_code_22') }} as procedure_code_22
    , {{ cast_string_or_varchar('px.procedure_code_23') }} as procedure_code_23
    , {{ cast_string_or_varchar('px.procedure_code_24') }} as procedure_code_24
    , {{ cast_string_or_varchar('px.procedure_code_25') }} as procedure_code_25
    , {{ try_to_cast_date('px.procedure_date_1', 'YYYY-MM-DD') }} as procedure_date_1
    , {{ try_to_cast_date('px.procedure_date_2', 'YYYY-MM-DD') }} as procedure_date_2
    , {{ try_to_cast_date('px.procedure_date_3', 'YYYY-MM-DD') }} as procedure_date_3
    , {{ try_to_cast_date('px.procedure_date_4', 'YYYY-MM-DD') }} as procedure_date_4
    , {{ try_to_cast_date('px.procedure_date_5', 'YYYY-MM-DD') }} as procedure_date_5
    , {{ try_to_cast_date('px.procedure_date_6', 'YYYY-MM-DD') }} as procedure_date_6
    , {{ try_to_cast_date('px.procedure_date_7', 'YYYY-MM-DD') }} as procedure_date_7
    , {{ try_to_cast_date('px.procedure_date_8', 'YYYY-MM-DD') }} as procedure_date_8
    , {{ try_to_cast_date('px.procedure_date_9', 'YYYY-MM-DD') }} as procedure_date_9
    , {{ try_to_cast_date('px.procedure_date_10', 'YYYY-MM-DD') }} as procedure_date_10
    , {{ try_to_cast_date('px.procedure_date_11', 'YYYY-MM-DD') }} as procedure_date_11
    , {{ try_to_cast_date('px.procedure_date_12', 'YYYY-MM-DD') }} as procedure_date_12
    , {{ try_to_cast_date('px.procedure_date_13', 'YYYY-MM-DD') }} as procedure_date_13
    , {{ try_to_cast_date('px.procedure_date_14', 'YYYY-MM-DD') }} as procedure_date_14
    , {{ try_to_cast_date('px.procedure_date_15', 'YYYY-MM-DD') }} as procedure_date_15
    , {{ try_to_cast_date('px.procedure_date_16', 'YYYY-MM-DD') }} as procedure_date_16
    , {{ try_to_cast_date('px.procedure_date_17', 'YYYY-MM-DD') }} as procedure_date_17
    , {{ try_to_cast_date('px.procedure_date_18', 'YYYY-MM-DD') }} as procedure_date_18
    , {{ try_to_cast_date('px.procedure_date_19', 'YYYY-MM-DD') }} as procedure_date_19
    , {{ try_to_cast_date('px.procedure_date_20', 'YYYY-MM-DD') }} as procedure_date_20
    , {{ try_to_cast_date('px.procedure_date_21', 'YYYY-MM-DD') }} as procedure_date_21
    , {{ try_to_cast_date('px.procedure_date_22', 'YYYY-MM-DD') }} as procedure_date_22
    , {{ try_to_cast_date('px.procedure_date_23', 'YYYY-MM-DD') }} as procedure_date_23
    , {{ try_to_cast_date('px.procedure_date_24', 'YYYY-MM-DD') }} as procedure_date_24
    , {{ try_to_cast_date('px.procedure_date_25', 'YYYY-MM-DD') }} as procedure_date_25
    , 'cclf' as data_source
from {{ var('parta_claims_header')}} h
inner join {{ var('parta_claims_revenue_center_detail')}} d
	on h.cur_clm_uniq_id = d.cur_clm_uniq_id
left join {{ ref('procedure_pivot')}} px
	on h.cur_clm_uniq_id = px.cur_clm_uniq_id
left join {{ ref('diagnosis_pivot')}} dx
	on h.cur_clm_uniq_id = dx.cur_clm_uniq_id