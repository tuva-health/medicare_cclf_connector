/* prep address details for concat */
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
        , nullif(trim(bene_entlmt_buyin_ind),'') as bene_entlmt_buyin_ind
        , bene_part_a_enrlmt_bgn_dt
        , bene_part_b_enrlmt_bgn_dt
        , bene_line_1_adr
        , case
            when bene_line_2_adr is null then ''
            else cast({{ dbt.concat(["', '","bene_line_2_adr"]) }} as {{ dbt.type_string() }} )
          end as bene_line_2_adr
        , case
            when bene_line_3_adr is null then ''
            else cast({{ dbt.concat(["', '","bene_line_3_adr"]) }} as {{ dbt.type_string() }} )
          end as bene_line_3_adr
        , case
            when bene_line_4_adr is null then ''
            else cast({{ dbt.concat(["', '","bene_line_4_adr"]) }} as {{ dbt.type_string() }} )
          end as bene_line_4_adr
        , case
            when bene_line_5_adr is null then ''
            else cast({{ dbt.concat(["', '","bene_line_5_adr"]) }} as {{ dbt.type_string() }} )
          end as bene_line_5_adr
        , case
            when bene_line_6_adr is null then ''
            else cast({{ dbt.concat(["', '","bene_line_6_adr"]) }} as {{ dbt.type_string() }} )
          end as bene_line_6_adr
        , geo_zip_plc_name
        , geo_usps_state_cd
        , geo_zip5_cd
        , case
            when geo_zip4_cd is null then ''
            else cast({{ dbt.concat(["'-'","geo_zip4_cd"]) }} as {{ dbt.type_string() }} )
            end as geo_zip4_cd
        , coverage_month
        , file_name
        , file_date
    from {{ ref('int_beneficiary_demographics_deduped') }}

)

, enrollment as (

    select
          current_bene_mbi_id
        , cast(enrollment_start_date as date) as enrollment_start_date
        , cast(enrollment_end_date as date) as enrollment_end_date
    from {{ ref('int_enrollment') }}

)

, joined as (

    select
          cast(demographics.current_bene_mbi_id as {{ dbt.type_string() }} ) as person_id
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
            when enrollment.enrollment_end_date >= cast({{ dbt.current_timestamp() }} as date)
            then {{ last_day(dbt.current_timestamp(), 'month') }}
            when enrollment.enrollment_end_date is null then {{ last_day(dbt.current_timestamp(), 'month') }}
            else cast(enrollment.enrollment_end_date as date)
          end as enrollment_end_date
        , 'medicare' as payer
        , 'medicare' as payer_type
        , 'medicare' as {{ the_tuva_project.quote_column('plan') }}
        , cast(demographics.bene_orgnl_entlmt_rsn_cd as {{ dbt.type_string() }} ) as original_reason_entitlement_code
        , cast(demographics.bene_dual_stus_cd as {{ dbt.type_string() }} ) as dual_status_code
        , cast(demographics.bene_mdcr_stus_cd as {{ dbt.type_string() }} ) as medicare_status_code
        , cast(null as {{ dbt.type_string() }} ) as group_id
        , cast(null as {{ dbt.type_string() }} ) as group_name
        , cast(demographics.bene_entlmt_buyin_ind as {{ dbt.type_string() }} ) as medicare_entitlement_buyin_indicator
        , cast(null as {{ dbt.type_string() }} ) as name_suffix
        , cast(demographics.bene_1st_name as {{ dbt.type_string() }} ) as first_name
        , cast(demographics.bene_midl_name as {{ dbt.type_string() }} ) as middle_name
        , cast(demographics.bene_last_name as {{ dbt.type_string() }} ) as last_name
        , cast(null as {{ dbt.type_string() }} ) as social_security_number
        , cast('self' as {{ dbt.type_string() }} ) as subscriber_relation
        , {{ dbt.concat(
            [
                "demographics.bene_line_1_adr",
                "demographics.bene_line_2_adr",
                "demographics.bene_line_3_adr",
                "demographics.bene_line_4_adr",
                "demographics.bene_line_5_adr",
                "demographics.bene_line_6_adr"
            ]
          ) }} as address
        , cast(demographics.geo_zip_plc_name as {{ dbt.type_string() }} ) as city
        , cast(demographics.bene_fips_state_cd as {{ dbt.type_string() }} ) as state
        , {{ dbt.concat(
            [
                "demographics.geo_zip5_cd",
                "demographics.geo_zip4_cd"
            ]
          ) }} as zip_code
        , cast(NULL as {{ dbt.type_string() }} ) as phone
        , cast(NULL as {{ dbt.type_string() }} ) as email
        , cast(NULL as {{ dbt.type_string() }} ) as ethnicity
        , 'medicare cclf' as data_source
        , cast(demographics.file_name as {{ dbt.type_string() }} ) as file_name
        , cast(NULL as date ) as file_date
        , cast(demographics.file_date as {{ dbt.type_timestamp() }} ) as ingest_datetime
    from enrollment
    left join demographics
        on demographics.current_bene_mbi_id = enrollment.current_bene_mbi_id
      -- Subtracting 1 month since files typically lag the month by 1 month
      -- Having prior years files come in January is indicative of this
        -- and dateadd(month, -1, demographics.coverage_month) = enrollment.member_month_date
        and demographics.coverage_month = datefromparts(year(enrollment.enrollment_end_date), month(enrollment.enrollment_end_date), 1)
)

select
      person_id
    , member_id
    , subscriber_id
    , gender
    , race
    , birth_date
    , death_date
    , death_flag
    , {{ extract_year('enrollment_start_date') }} as reference_year
    , enrollment_start_date
    , enrollment_end_date
    , payer
    , payer_type
    , {{ the_tuva_project.quote_column('plan') }}
    , original_reason_entitlement_code
    , dual_status_code
    , medicare_status_code
    , group_id
    , group_name
    , nullif(trim(medicare_entitlement_buyin_indicator),'') as medicare_entitlement_buyin_indicator
    , name_suffix
    , first_name
    , middle_name
    , last_name
    , social_security_number
    , subscriber_relation
    , address
    , city
    , nullif(trim(state),'') as state
    , zip_code
    , phone
    , email
    , ethnicity
    , data_source
    , file_name
    , file_date
    , ingest_datetime
from joined