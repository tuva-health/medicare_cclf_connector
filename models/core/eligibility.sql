{{
    config( materialized='ephemeral' )
}}

select 
	cast(bene_mbi_id as varchar) as patient_id
    ,cast(case bene_sex_cd
          when '0' then 'unknown'
          when '1' then 'male'
          when '2' then 'female'
     end as varchar) as gender
    ,cast(bene_dob as datetime) as birth_date
    ,cast(case bene_race_cd
          when '0' then 'unknown'
          when '1' then 'white'
          when '2' then 'black'
          when '3' then 'other'
          when '4' then 'asian'
          when '5' then 'hispanic'
          when '6' then 'north american native'
     end as varchar) as race
    ,cast(bene_zip_cd as varchar) as zip_code
    ,cast(sf.state as varchar) as state
    ,cast(case 
      	  when bene_death_dt is null then 0
		  else 1
     end as int) as deceased_flag
    ,cast(bene_death_dt as datetime) as death_date
    ,cast('medicare' as varchar) as payer
    ,cast('medicare' as varchar) as payer_type
    ,cast(bene_dual_stus_cd as varchar) as dual_status
    ,cast(bene_mdcr_stus_cd as varchar) as medicare_status
    ,cast(date_part(month, bene_member_month) as int) as month
    ,cast(date_part(year, bene_member_month) as int) as year
from {{ var('beneficiary_demographics')}} b
left join {{ ref('medicare_state_fips')}} sf
	on b.bene_fips_state_cd = sf.fips_code