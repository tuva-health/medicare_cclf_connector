with encounter_dates as(
  select
    encounter_id
    ,bene_mbi_id as patient_id
    ,min(clm_from_dt) as encounter_start_date
    ,max(clm_thru_dt) as encounter_end_date
    ,cast(sum(clm_line_cvrd_pd_amt) as float) as paid_amount
  from {{ ref('prof_claims_final')}}
  group by 
    encounter_id
    ,bene_mbi_id
)
,encounter_stage as(
select
	cast(e.encounter_id as varchar) as encounter_id
  , cast(e.patient_id as varchar) as patient_id
  , case 
  		when i.encounter_type is not null then i.encounter_type
      when f.clm_type_cd in ('72', '82') then 'dme'
        when s.description = 'Office' then 'office visit'
        when s.description = 'Independent Laboratory' then 'laboratory'
        when s.description = 'Inpatient Hospital' then 'acute inpatient'
        when s.description = 'On Campus Outpatient Hospital' then 'outpatient'
        when s.description = 'Emergency Room Hospital' then 'emergency'
        when s.description = 'Ambulatory Surgical Center' then 'ambulatory surgical center'
        when s.description = 'Nursing Facility' then 'skilled nursing facility'
        when s.description = 'Skilled Nursing Facility' then 'skilled nursing facility'
        when s.description = 'Mass Immunization Center' then 'other'
        when s.description = 'Ambulance - Land' then 'other'
        when s.description = 'Home' then 'home health'
        when s.description = 'Assisted Living Facility' then 'home health'
        when s.description = 'Urgent Care Facility' then 'emergency'
        when s.description = 'Off Campus Outpatient Hospital' then 'outpatient'
        when s.description = 'Other Place of Service' then 'other'
        when s.description = 'End-stage Renal Disease Treatment Facility' then 'dialysis'
        when s.description = 'Independent Clinic' then 'office visit'
        when s.description = 'Community Mental Health Center' then 'community mental health center'
        when s.description = 'Custodial Care Facility' then 'home health'
        when s.description = 'Comprehensive Inpatient Rehabilitation Facility' then 'inpatient rehab'
        when s.description = 'Telehealth (provided other than in patient''s home)' then 'other'
        when s.description = 'Group Home' then 'home health'
        when s.description = 'Public Health Clinic' then 'office visit'
        when s.description = 'Inpatient Psychiatric Facility' then 'psychiatric'
        when s.description = 'Mobile Unit' then 'other'
        when s.description = 'Intermediate Care Facility / Individuals with intellectual disabilities' then 'other'
        when s.description = 'Psychiatric Facility - Partial Hospitalization' then 'psychiatric'
        when s.description = 'Federally Qualified Health Center' then 'other'
        when s.description = 'Ambulance - Air or Water' then 'other'
        when s.description = 'Walk-in Retail Health Clinic' then 'office visit'
        when s.description = 'Rural Health Clinic' then 'office visit'
        when s.description = 'Pharmacy' then 'pharmacy'
        when s.description = 'Non-residential Substance Abuse Treatment Facility' then 'other'
        when s.description = 'Psychiatric Residential Treatment Center' then 'psychiatric'
        when s.description = 'Temporary Lodging' then 'other'
        when s.description = 'Comprehensive Outpatient Rehabilitation Facility' then 'outpatient rehab'
        when s.description = 'School' then 'other'
        when s.description = 'Hospice' then 'hospice'
        when s.description = 'Residential Substance Abuse Treatment Center' then 'other'
        when s.description = 'Place of Employment Worksite' then 'other'
        when s.description = 'Prison or correctional facility' then 'other'
        when s.description = 'Birthing Center' then 'other'
        when s.description = 'Homeless Shelter' then 'other'
        when s.description = 'Tribal 638 (provider-based facility)' then 'other'
        when s.description = 'Military Treatment Facility' then 'other'
        when s.description = 'Indian Health Service (free standing facility)' then 'other'
        when s.description = 'Tribal 638 (free-standing facility)' then 'other'
        when s.description = 'Indian Health Service (provider-based facility)' then 'other'
  	end as encounter_type
  , cast(e.encounter_start_date as varchar) as encounter_start_date
  , cast(e.encounter_end_date as varchar) as encounter_end_date
  , cast(NULL as varchar) as admit_type_code
  , cast(NULL as varchar) as admit_type_description
  , cast(NULL as varchar) as admit_source_code
  , cast(NULL as varchar) as admit_source_description
  , cast(NULL as varchar) as discharge_disposition_code
  , cast(NULL as varchar) as discharge_disposition_description
  , cast(f.rndrg_prvdr_npi_num as varchar) as physician_npi
  , cast(NULL as varchar) as location
  , cast(NULL as varchar) as facility_npi
  , cast(NULL as varchar) as ms_drg
  , cast(e.paid_amount as varchar) as paid_amount
  , cast('cclf' as varchar) as data_source
  , cast(row_number() over (partition by f.encounter_id order by f.cur_clm_uniq_id) as int) as row_number
from {{ ref('prof_claims_final')}} f
inner join encounter_dates e
	on f.encounter_id = e.encounter_id
left join {{ ref('inst_encounter_core')}} i
  	on f.encounter_id = i.encounter_id
left join {{ ref('place_of_service')}} s
  	on f.clm_pos_cd = s.place_of_service_code
where f.clm_line_num = 1
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