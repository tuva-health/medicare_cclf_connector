/**  Date of birth has been rolled up the they beginning of the year since SAF only provides year .
		Patients can have multiple addresses, using most recent based on member month   **/

with stage_patient as(
  select distinct
     cast(bene_mbi_id as varchar) as patient_id
    , cast(NULL as varchar) as name
    , cast(
      case bene_sex_cd
          when 1 then 'male'
          when 2 then'female'
          when 0 then 'unknown'
      end as varchar) as gender
    , cast(
      case bene_race_cd
          when 0 then 'unknown'
          when 1 then 'white'
          when 2 then 'black'
          when 3 then 'other'
          when 4 then 'asian'
          when 5 then 'other'  -- CMS def Hispanic
          when 6 then 'american indian or alaska native' -- CMS def North American Native
      end as varchar) as race
    , cast(
      case bene_race_cd
          when 6 then 'hispanic or latino'
      end as varchar) as ethnicity
   -- , cast(bene_mdcr_stus_cd as varchar) as medicare_status
    --, cast(bene_dual_stus_cd as varchar) as dual_status
    , cast(bene_dob as datetime) as birth_date
    , cast(bene_death_dt as datetime) as death_date
    , cast(
      case bene_death_dt 
          when null then 0
              else 1
      end as varchar) as death_flag
    , cast(NULL as varchar) as address
    , cast(NULL as varchar) as city
    , cast(c.state_name as varchar) as state
    , cast(NULL as varchar) as zip_code
    , cast(NULL as int) as phone
    , cast(NULL as varchar) as email
    , cast(NULL as varchar) as ssn
    , cast('cclf' as varchar) as data_source
    , row_number() over (partition by bene_mbi_id order by bene_member_month DESC) as row_number
  from {{ source('medicare_cclf','beneficiary_demographics')}} b
  left join {{ ref('saf_state_codes')}} c
    on b.geo_usps_state_cd = c.state_code
)
select
   cast(patient_id as varchar) as patient_id
  , cast(name as varchar) as name
  , cast(gender as varchar) as gender
  , cast(race as varchar) as race
  , cast(ethnicity as varchar) as ethnicity
  , cast(birth_date as varchar) as birth_date
  , cast(death_date as varchar) as death_date
  , cast(death_flag as varchar) as death_flag
  , cast(address as varchar) as address
  , cast(city as varchar) as city
  , cast(state as varchar) as state
  , cast(zip_code as varchar) as zip_code
  , cast(phone as varchar) as phone
  , cast(email as varchar) as email
  , cast(ssn as varchar) as ssn
  , cast(data_source as varchar) as data_source
from stage_patient
where row_number = 1