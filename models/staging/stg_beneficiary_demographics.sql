-- CTE that selects from either the source table or the demo data seed based on the 'demo_data_only' variable
with beneficiary_demographics as (
  select pi.value as bene_mbi_id, * 
  from {{ source('fhir', 'patient')}} p
  inner join {{ source('fhir', 'patient_identifier')}} pi 
    on pi.patient_id = p.id
  where pi.system = 'http://hl7.org/fhir/sid/us-mbi'
)


{% set current_year = modules.datetime.date.today().year %}
{% for month in range(1, 13) %}

select
      bene_mbi_id
    , null as bene_hic_num
    , null as bene_fips_state_cd
    , null as bene_fips_cnty_cd
    , null as bene_zip_cd
    , {{ try_to_cast_date('birth_date') }} as bene_dob
    , gender as bene_sex_cd
    , race as bene_race_cd
    , null as bene_age
    , null as bene_mdcr_stus_cd
    , null as bene_dual_stus_cd
    , deceased_datetime as bene_death_dt
    , null as bene_rng_bgn_dt
    , null as bene_rng_end_dt
    , name_given_1 as bene_1st_name
    , null as bene_midl_name
    , name_family as bene_last_name
    , null as bene_orgnl_entlmt_rsn_cd
    , null as bene_entlmt_buyin_ind
    , null as bene_part_a_enrlmt_bgn_dt
    , null as bene_part_b_enrlmt_bgn_dt
    , null as bene_line_1_adr
    , null as bene_line_2_adr
    , null as bene_line_3_adr
    , null as bene_line_4_adr
    , null as bene_line_5_adr
    , null as bene_line_6_adr
    , null as geo_zip_plc_name
    , null as geo_usps_state_cd
    , null as geo_zip5_cd
    , null as geo_zip4_cd
    , null as file_name
    , date_from_parts({{ current_year }}, {{ month }}, 1) as file_date
from beneficiary_demographics

  {% if not loop.last %}
  union all
  {% endif %}
{% endfor %}
