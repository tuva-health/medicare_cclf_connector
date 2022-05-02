
  select distinct
      fac_prvdr_npi_num as facility_npi
    , NULL as facility_name
    , NULL as facility_type
    , NULL as hospital_type
    , NULL as parent_organization
    , 'cclf' as data_source
  from {{ ref('inst_claims_final')}}