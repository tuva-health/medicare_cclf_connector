/**  Creating a final list of encounter.  Since prof was crosswalked to inst, prioritizing inst since this is the 'header'
		but combining paid amounts to represent total of all services.  **/

select 
   cast(coalesce(i.encounter_id, p.encounter_id) as varchar) as encounter_id
  , cast(coalesce(i.patient_id, p.patient_id) as varchar) as patient_id
  , cast(coalesce(i.encounter_type, p.encounter_type) as varchar) as encounter_type
  , cast(coalesce(i.encounter_start_date, p.encounter_start_date) as date) as encounter_start_date
  , cast(coalesce(i.encounter_end_date, p.encounter_end_date) as date) as encounter_end_date
  , cast(coalesce(i.admit_source_code, p.admit_source_code) as varchar) as admit_source_code
  , cast(coalesce(i.admit_source_description, p.admit_source_description) as varchar) as admit_source_description
  , cast(coalesce(i.admit_type_code, p.admit_type_code) as varchar) as admit_type_code
  , cast(coalesce(i.admit_type_description, p.admit_type_description) as varchar) as admit_type_description
  , cast(coalesce(i.discharge_disposition_code, p.discharge_disposition_code) as varchar) as discharge_disposition_code
  , cast(coalesce(i.discharge_disposition_description, p.discharge_disposition_description) as varchar) as discharge_disposition_description
  , cast(coalesce(i.physician_npi, p.physician_npi) as varchar) as physician_npi
  , cast(coalesce(i.location, p.location) as varchar) as location
  , cast(coalesce(i.facility_npi, p.facility_npi) as varchar) as facility_npi 
  , cast(coalesce(i.ms_drg, p.ms_drg) as varchar) as ms_drg
  , cast(coalesce(i.paid_amount+p.paid_amount, p.paid_amount) as float) as paid_amount
  , cast(coalesce(i.data_source, p.data_source) as varchar) as data_source
from {{ ref('inst_encounter_core')}} i
full join {{ ref('prof_encounter_core')}} p
on i.encounter_id = p.encounter_id