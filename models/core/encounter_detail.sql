with stage as(

  select
    cast(encounter_id as varchar) as encounter_id
    , cast(encounter_detail_id as varchar) as encounter_detail_id
    , cast(clm_line_num as int) as encounter_detail_line
    , cast(bene_mbi_id as varchar) as patient_id
    , cast(r.description as varchar) as encounter_detail_type
    , cast(clm_from_dt as date) as detail_start_date
    , cast(clm_thru_dt as date) as detail_end_date
    , cast(clm_line_instnl_rev_ctr_dt as date) as revenue_center_date
    , cast(clm_line_srvc_unit_qty as varchar) as service_unit_quantity
    , cast(clm_line_cvrd_pd_amt as float) as detail_paid_amount
    , cast(clm_line_hcpcs_cd as varchar) as hcpcs_code
    , cast(hcpcs_1_mdfr_cd as varchar) as hcpcs_modifier_1
    , cast(hcpcs_2_mdfr_cd as varchar) as hcpcs_modifier_2
    , cast(hcpcs_3_mdfr_cd as varchar) as hcpcs_modifier_3
    , cast(hcpcs_4_mdfr_cd as varchar) as hcpcs_modifier_4
    , cast(hcpcs_5_mdfr_cd as varchar) as hcpcs_modifier_5
    , cast(NULL as varchar) as physician_npi
  from {{ ref('inst_revenue_center_final')}} f
  left join {{ ref('revenue_center')}}  r
  	on f.clm_line_prod_rev_ctr_cd = r.revenue_code
  
union all 
  
  select
      cast(encounter_id as varchar) as encounter_id
    , cast(encounter_detail_id as varchar) as encounter_detail_id
    , cast(clm_line_num as int) as encounter_detail_line
    , cast(bene_mbi_id as varchar) as patient_id
    , cast(s.description as varchar) as encounter_detail_type
    , cast(clm_line_from_dt as date) as detail_start_date
    , cast(clm_line_thru_dt as date) as detail_end_date
    , cast(NULL as date) as revenue_center_date
    , cast(clm_line_srvc_unit_qty as varchar) as service_unit_quantity
    , cast(clm_line_cvrd_pd_amt as float) as detail_paid_amount
    , cast(clm_line_hcpcs_cd as varchar) as hcpcs_code
    , cast(hcpcs_1_mdfr_cd as varchar) as hcpcs_modifier_1
    , cast(hcpcs_2_mdfr_cd as varchar) as hcpcs_modifier_2
    , cast(hcpcs_3_mdfr_cd as varchar) as hcpcs_modifier_3
    , cast(hcpcs_4_mdfr_cd as varchar) as hcpcs_modifier_4
    , cast(hcpcs_5_mdfr_cd as varchar) as hcpcs_modifier_5
    , cast(rndrg_prvdr_npi_num as varchar) as physician_npi
  from {{ ref('prof_claims_final')}} f
  left join {{ ref('place_of_service')}} s
  	on f.clm_pos_cd = s.place_of_service_code

union all 
  
  select
      cast(bene_mbi_id || replace(clm_thru_dt,'-','') || clm_pos_cd || clm_type_cd || payto_prvdr_npi_num as varchar) as encounter_id
    , cast(bene_mbi_id || cur_clm_uniq_id || clm_line_num || clm_type_cd as varchar) as encounter_detail_id
    , cast(clm_line_num as int) as encounter_detail_line
    , cast(bene_mbi_id as varchar) as patient_id
    , cast(s.description + ' - dme' as varchar) as encounter_detail_type
    , cast(clm_line_from_dt as date) as detail_start_date
    , cast(clm_line_thru_dt as date) as detail_end_date
    , cast(NULL as date) as revenue_center_date
    , cast(NULL as varchar) as service_unit_quantity
    , cast(clm_line_cvrd_pd_amt as float) as detail_paid_amount
    , cast(clm_line_hcpcs_cd as varchar) as hcpcs_code
    , cast(NULL as varchar) as hcpcs_modifier_1
    , cast(NULL as varchar) as hcpcs_modifier_2
    , cast(NULL as varchar) as hcpcs_modifier_3
    , cast(NULL as varchar) as hcpcs_modifier_4
    , cast(NULL as varchar) as hcpcs_modifier_5
    , cast(NULL as varchar) as physician_npi
  from {{ source('medicare_cclf','partb_dme')}} f
  left join {{ ref('place_of_service')}} s
  	on f.clm_pos_cd = s.place_of_service_code
  )
  select * from stage