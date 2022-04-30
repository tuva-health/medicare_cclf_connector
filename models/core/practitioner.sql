with specialty_stage as(
/** Using specialty from professional claims to fill in specialty for institutional **/
  select 
    rndrg_prvdr_npi_num as npi
    , NULL as name
    , s.specialty_name as specialty
    , NULL as sub_specialty
  from {{ ref('prof_claims_final')}} p
  left join {{ref ('provider_specialty')}} s
      on p.clm_prvdr_spclty_cd = s.specialty_code
)
, union_stage as(
  select 
    payto_prvdr_npi_num as npi
    , NULL as name
    , NULL as specialty
    , NULL as sub_specialty
  from {{ ref('prof_dme_final')}}

  union

  select 
      ordrg_prvdr_npi_num as npi
    , NULL as name
    , NULL as specialty
    , NULL as sub_specialty
  from {{ ref('prof_dme_final')}}

  union

  select 
      oprtg_prvdr_npi_num as npi
    , NULL as name
    , NULL as specialty
    , NULL as sub_specialty
  from {{ ref('inst_claims_final')}}

  union

  select 
      atndg_prvdr_npi_num as npi
    , NULL as name
    , NULL as specialty
    , NULL as sub_specialty
  from {{ ref('inst_claims_final')}}

  union

  select 
      othr_prvdr_npi_num as npi
    , NULL as name
    , NULL as specialty
    , NULL as sub_specialty
  from {{ ref('inst_claims_final')}}
)

select 
	u.npi
    ,u.name
    ,s.specialty
    ,s.sub_specialty
    ,'cclf' as data_source
from union_stage u
left join specialty_stage s
	on u.npi = s.npi

union

select 
	npi
    ,name
    ,specialty
    ,sub_specialty
    ,'cclf' as data_source
from specialty_stage

