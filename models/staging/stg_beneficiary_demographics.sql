select
      cast(bene_mbi_id as {{ dbt.type_string() }}) as bene_mbi_id
    , cast(bene_member_month as {{ dbt.type_string() }}) as bene_member_month
    , cast(bene_hic_num as {{ dbt.type_string() }}) as bene_hic_num
    , cast(bene_fips_state_cd as {{ dbt.type_string() }}) as bene_fips_state_cd
    , cast(bene_fips_cnty_cd as {{ dbt.type_string() }}) as bene_fips_cnty_cd
    , cast(bene_zip_cd as {{ dbt.type_string() }}) as bene_zip_cd
    , cast(bene_dob as {{ dbt.type_string() }}) as bene_dob
    , cast(bene_sex_cd as {{ dbt.type_string() }}) as bene_sex_cd
    , cast(bene_race_cd as {{ dbt.type_string() }}) as bene_race_cd
    , cast(bene_age as {{ dbt.type_string() }}) as bene_age
    , cast(bene_mdcr_stus_cd as {{ dbt.type_string() }}) as bene_mdcr_stus_cd
    , cast(bene_dual_stus_cd as {{ dbt.type_string() }}) as bene_dual_stus_cd
    , cast(bene_death_dt as {{ dbt.type_string() }}) as bene_death_dt
    , cast(bene_rng_bgn_dt as {{ dbt.type_string() }}) as bene_rng_bgn_dt
    , cast(bene_rng_end_dt as {{ dbt.type_string() }}) as bene_rng_end_dt
    , cast(bene_1st_name as {{ dbt.type_string() }}) as bene_1st_name
    , cast(bene_midl_name as {{ dbt.type_string() }}) as bene_midl_name
    , cast(bene_last_name as {{ dbt.type_string() }}) as bene_last_name
    , cast(bene_orgnl_entlmt_rsn_cd as {{ dbt.type_string() }}) as bene_orgnl_entlmt_rsn_cd
    , cast(bene_entlmt_buyin_ind as {{ dbt.type_string() }}) as bene_entlmt_buyin_ind
    , cast(bene_part_a_enrlmt_bgn_dt as {{ dbt.type_string() }}) as bene_part_a_enrlmt_bgn_dt
    , cast(bene_part_b_enrlmt_bgn_dt as {{ dbt.type_string() }}) as bene_part_b_enrlmt_bgn_dt
    , cast(bene_line_1_adr as {{ dbt.type_string() }}) as bene_line_1_adr
    , cast(bene_line_2_adr as {{ dbt.type_string() }}) as bene_line_2_adr
    , cast(bene_line_3_adr as {{ dbt.type_string() }}) as bene_line_3_adr
    , cast(bene_line_4_adr as {{ dbt.type_string() }}) as bene_line_4_adr
    , cast(bene_line_5_adr as {{ dbt.type_string() }}) as bene_line_5_adr
    , cast(bene_line_6_adr as {{ dbt.type_string() }}) as bene_line_6_adr
    , cast(geo_zip_plc_name as {{ dbt.type_string() }}) as geo_zip_plc_name
    , cast(geo_usps_state_cd as {{ dbt.type_string() }}) as geo_usps_state_cd
    , cast(geo_zip5_cd as {{ dbt.type_string() }}) as geo_zip5_cd
    , cast(geo_zip4_cd as {{ dbt.type_string() }}) as geo_zip4_cd
    , cast(file_name as {{ dbt.type_string() }}) as file_name
    , cast(ingest_datetime as {{ dbt.type_timestamp() }}) as ingest_datetime
from {{ source('medicare_cclf','beneficiary_demographics') }}