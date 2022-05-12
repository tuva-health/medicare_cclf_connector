with encounter_dates as(
  select
    encounter_id
    ,bene_mbi_id as patient_id
    ,min(clm_from_dt) as encounter_start_date
    ,max(clm_thru_dt) as encounter_end_date
    ,cast(sum(total_payment_amount) as float) as paid_amount
  from {{ ref('inst_claims_final')}}
  group by 
    encounter_id
    ,bene_mbi_id
)
,encounter_stage as(
select
	cast(e.encounter_id as varchar) as encounter_id
  , cast(e.patient_id as varchar) as patient_id
  , cast(
        case when clm_bill_fac_type_cd = '1' and clm_bill_clsfctn_cd = '8' then 'skilled nursing facility'
        when clm_bill_fac_type_cd = '1' and clm_bill_clsfctn_cd = '2' then 'acute inpatient'
        when clm_bill_fac_type_cd = '1' and clm_bill_clsfctn_cd = '3' then 'outpatient'
        when clm_bill_fac_type_cd = '1' and clm_bill_clsfctn_cd = '4' then 'outpatient'
        when clm_bill_fac_type_cd = '1' and clm_bill_clsfctn_cd = '1' then 'acute inpatient'
        when clm_bill_fac_type_cd = '2' and clm_bill_clsfctn_cd = '1' then 'skilled nursing facility'
        when clm_bill_fac_type_cd = '2' and clm_bill_clsfctn_cd = '2' then 'skilled nursing facility'
        when clm_bill_fac_type_cd = '2' and clm_bill_clsfctn_cd = '3' then 'skilled nursing facility'
        when clm_bill_fac_type_cd = '3' and clm_bill_clsfctn_cd = '2' then 'home health'
        when clm_bill_fac_type_cd = '3' and clm_bill_clsfctn_cd = '4' then 'home health'
        when clm_bill_fac_type_cd = '4' and clm_bill_clsfctn_cd = '1' then 'acute inpatient'
        when clm_bill_fac_type_cd = '7' and clm_bill_clsfctn_cd = '5' then 'outpatient rehab'
        when clm_bill_fac_type_cd = '7' and clm_bill_clsfctn_cd = '1' then 'rural health clinical'
        when clm_bill_fac_type_cd = '7' and clm_bill_clsfctn_cd = '6' then 'community mental health center'
        when clm_bill_fac_type_cd = '7' and clm_bill_clsfctn_cd = '7' then 'other'
        when clm_bill_fac_type_cd = '7' and clm_bill_clsfctn_cd = '2' then 'dialysis'
        when clm_bill_fac_type_cd = '7' and clm_bill_clsfctn_cd = '4' then 'other rehab'
        when clm_bill_fac_type_cd = '8' and clm_bill_clsfctn_cd = '5' then 'outpatient rehab'
        when clm_bill_fac_type_cd = '8' and clm_bill_clsfctn_cd = '1' then 'hospice'
        when clm_bill_fac_type_cd = '8' and clm_bill_clsfctn_cd = '2' then 'hospice'
  			else 'other'
    end 
    as varchar) as encounter_type
  , cast(e.encounter_start_date as varchar) as encounter_start_date
  , cast(e.encounter_end_date as varchar) as encounter_end_date
  , cast(clm_admsn_type_cd as varchar) as admit_type_code
  , cast(NULL as varchar) as admit_type_description
  , cast(clm_admsn_src_cd as varchar) as admit_source_code
  , cast(NULL as varchar) as admit_source_description
  , cast(bene_ptnt_stus_cd as varchar) as discharge_disposition_code
  , cast(d.description as varchar) as discharge_disposition_description
  , cast(f.atndg_prvdr_npi_num as varchar) as physician_npi
  , cast(NULL as varchar) as location
  , cast(fac_prvdr_npi_num as varchar) as facility_npi
  , cast(dgns_drg_cd as varchar) as ms_drg
  , cast(e.paid_amount as varchar) as paid_amount
  , cast('cclf' as varchar) as data_source
  , cast(row_number() over (partition by f.encounter_id order by f.cur_clm_uniq_id) as int) as row_number
from {{ ref('inst_claims_final')}} f
inner join encounter_dates e
	on f.encounter_id = e.encounter_id
left join {{ ref('discharge_disposition')}} d
  on f.bene_ptnt_stus_cd = d.discharge_disposition_code
)

select 
 cast(encounter_id as varchar) as encounter_id
, cast(patient_id as varchar) as patient_id
, cast(encounter_type as varchar) as encounter_type
, cast(encounter_start_date as date) as encounter_start_date
, cast(encounter_end_date as date) as encounter_end_date
, cast(admit_source_code as varchar) as admit_source_code
, cast(admit_source_description as varchar) as admit_source_description
, cast(admit_type_code as varchar) as admit_type_code
, cast(admit_type_description as varchar) as admit_type_description
, cast(discharge_disposition_code as varchar) as discharge_disposition_code
, cast(discharge_disposition_description as varchar) as discharge_disposition_description
, cast(physician_npi as varchar) as physician_npi
, cast(location as varchar) as location
, cast(facility_npi as varchar) as facility_npi 
, cast(ms_drg as varchar) as ms_drg
, cast(paid_amount as float) as paid_amount
, cast(data_source as varchar) as data_source
from encounter_stage 
where row_number = 1