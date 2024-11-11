with demographics as (

    select
          current_bene_mbi_id
        , bene_hic_num
        , bene_fips_state_cd
        , bene_fips_cnty_cd
        , bene_zip_cd
        , bene_dob
        , bene_sex_cd
        , bene_race_cd
        , bene_mdcr_stus_cd
        , bene_dual_stus_cd
        , bene_death_dt
        , bene_rng_bgn_dt
        , bene_rng_end_dt
        , bene_1st_name
        , bene_midl_name
        , bene_last_name
        , bene_orgnl_entlmt_rsn_cd
        , bene_entlmt_buyin_ind
        , bene_part_a_enrlmt_bgn_dt
        , bene_part_b_enrlmt_bgn_dt
        , bene_line_1_adr
        , bene_line_2_adr
        , bene_line_3_adr
        , bene_line_4_adr
        , bene_line_5_adr
        , bene_line_6_adr
        , geo_zip_plc_name
        , geo_usps_state_cd
        , geo_zip5_cd
        , geo_zip4_cd
        , file_name
        , file_date
    from {{ ref('int_beneficiary_demographics_deduped') }}

)

, enrollment as (

    select
          current_bene_mbi_id
        , enrollment_start_date
        , enrollment_end_date
    from {{ ref('int_enrollment') }}

)

, joined as (

    select
          cast(demographics.current_bene_mbi_id as {{ dbt.type_string() }} ) as patient_id
        , cast(demographics.current_bene_mbi_id as {{ dbt.type_string() }} ) as member_id
        , cast(null as {{ dbt.type_string() }} ) as subscriber_id
        , case demographics.bene_sex_cd
            when '0' then 'unknown'
            when '1' then 'male'
            when '2' then 'female'
          end as gender
        , case demographics.bene_race_cd
            when '0' then 'unknown'
            when '1' then 'white'
            when '2' then 'black'
            when '3' then 'other'
            when '4' then 'asian'
            when '5' then 'hispanic'
            when '6' then 'north american native'
          end as race
        , {{ try_to_cast_date('demographics.bene_dob', 'YYYY-MM-DD') }} as birth_date
        , {{ try_to_cast_date('demographics.bene_death_dt', 'YYYY-MM-DD') }} as death_date
        , cast(case
               when demographics.bene_death_dt is null then 0
               else 1
          end as integer) as death_flag
        , cast(enrollment.enrollment_start_date as date) as enrollment_start_date
        , case
            when enrollment.enrollment_end_date >= current_date then last_day(current_date, 'month')
            when enrollment.enrollment_end_date is null then last_day(current_date, 'month')
            else cast(enrollment.enrollment_end_date as date)
          end as enrollment_end_date
        , 'medicare' as payer
        , 'medicare' as payer_type
        , 'medicare' as plan
        , cast(demographics.bene_orgnl_entlmt_rsn_cd as {{ dbt.type_string() }} ) as original_reason_entitlement_code
        , cast(demographics.bene_dual_stus_cd as {{ dbt.type_string() }} ) as dual_status_code
        , cast(demographics.bene_mdcr_stus_cd as {{ dbt.type_string() }} ) as medicare_status_code
        , cast(demographics.bene_1st_name as {{ dbt.type_string() }} ) as first_name
        , cast(demographics.bene_last_name as {{ dbt.type_string() }} ) as last_name
        , cast(null as {{ dbt.type_string() }} ) as social_security_number
        , cast('self' as {{ dbt.type_string() }} ) as subscriber_relation
        , cast(demographics.bene_line_1_adr as {{ dbt.type_string() }} )
            || case when demographics.bene_line_2_adr is not null then ', '|| cast(demographics.bene_line_2_adr as {{ dbt.type_string() }} ) else '' end
            || case when demographics.bene_line_3_adr is not null then ', '|| cast(demographics.bene_line_3_adr as {{ dbt.type_string() }} ) else '' end
            || case when demographics.bene_line_4_adr is not null then ', '|| cast(demographics.bene_line_4_adr as {{ dbt.type_string() }} ) else '' end
            || case when demographics.bene_line_5_adr is not null then ', '|| cast(demographics.bene_line_5_adr as {{ dbt.type_string() }} ) else '' end
            || case when demographics.bene_line_6_adr is not null then ', '|| cast(demographics.bene_line_6_adr as {{ dbt.type_string() }} ) else '' end
          as address
        , cast(demographics.geo_zip_plc_name as {{ dbt.type_string() }} ) as city
        , cast(demographics.geo_usps_state_cd as {{ dbt.type_string() }} ) as state
        , cast(demographics.geo_zip5_cd as {{ dbt.type_string() }} )
            || case when demographics.geo_zip4_cd is not null then '-'||cast(demographics.geo_zip4_cd as {{ dbt.type_string() }} ) else '' end
          as zip_code
        , cast(NULL as {{ dbt.type_string() }} ) as phone
        , 'medicare cclf' as data_source
        , cast(demographics.file_name as {{ dbt.type_string() }} ) as file_name
        , cast(demographics.file_date as {{ dbt.type_timestamp() }} ) as ingest_datetime
    from demographics
        left join enrollment
            on demographics.current_bene_mbi_id = enrollment.current_bene_mbi_id

)

select
      patient_id
    , member_id
    , subscriber_id
    , gender
    , race
    , birth_date
    , death_date
    , death_flag
    , enrollment_start_date
    , enrollment_end_date
    , payer
    , payer_type
    , plan
    , original_reason_entitlement_code
    , dual_status_code
    , medicare_status_code
    , first_name
    , last_name
    , social_security_number
    , subscriber_relation
    , address
    , city
    , state
    , zip_code
    , phone
    , data_source
    , file_name
    , ingest_datetime
from joined