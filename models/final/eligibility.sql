{% if var('cms_alr_connector', false) %}

with member_months as (

    select *
    from {{ ref('int_eligibility_member_months_combined') }}

)

, add_row_num as (

    select
          member_months.*
        , row_number() over (
            partition by current_bene_mbi_id
            order by coverage_month
          ) as row_num
    from member_months

)

, add_lag_enrollment as (

    select
          current_bene_mbi_id
        , coverage_month
        , eligibility_flag
        , data_sharing_flag
        , row_num
        , lag(coverage_month) over (
            partition by current_bene_mbi_id
            order by row_num
          ) as lag_enrollment
        , lag(eligibility_flag) over (
            partition by current_bene_mbi_id
            order by row_num
          ) as lag_eligibility_flag
        , lag(data_sharing_flag) over (
            partition by current_bene_mbi_id
            order by row_num
          ) as lag_data_sharing_flag
    from add_row_num

)

, calculate_lag_diff as (

    select
          current_bene_mbi_id
        , coverage_month
        , eligibility_flag
        , data_sharing_flag
        , row_num
        , lag_enrollment
        , lag_eligibility_flag
        , lag_data_sharing_flag
        , {{ datediff('lag_enrollment', 'coverage_month', 'month') }} as lag_diff
    from add_lag_enrollment

)

, calculate_gaps as (

    select
          current_bene_mbi_id
        , coverage_month
        , eligibility_flag
        , data_sharing_flag
        , row_num
        , lag_enrollment
        , lag_eligibility_flag
        , lag_data_sharing_flag
        , lag_diff
        , case
            when lag_diff > 1 then 1
            when coalesce(eligibility_flag, 0) != coalesce(lag_eligibility_flag, 0) then 1
            when coalesce(data_sharing_flag, 0) != coalesce(lag_data_sharing_flag, 0) then 1
            else 0
          end as gap_flag
    from calculate_lag_diff

)

, calculate_groups as (

    select
          current_bene_mbi_id
        , coverage_month
        , eligibility_flag
        , data_sharing_flag
        , row_num
        , lag_enrollment
        , lag_eligibility_flag
        , lag_data_sharing_flag
        , lag_diff
        , gap_flag
        , sum(gap_flag) over (
            partition by current_bene_mbi_id
            order by row_num
            rows between unbounded preceding and current row
          ) as row_group
    from calculate_gaps

)

, rollup_groups as (

    select
          current_bene_mbi_id
        , eligibility_flag
        , data_sharing_flag
        , row_group
        , min(coverage_month) as enrollment_start_date
        , {{ last_day('max(coverage_month)', 'month') }} as enrollment_end_date
    from (
        select
              add_row_num.current_bene_mbi_id
            , add_row_num.eligibility_flag
            , add_row_num.data_sharing_flag
            , calculate_groups.row_group
            , add_row_num.coverage_month
        from add_row_num
        inner join calculate_groups
            on add_row_num.current_bene_mbi_id = calculate_groups.current_bene_mbi_id
            and add_row_num.coverage_month = calculate_groups.coverage_month
    ) grouped_months
    group by
          current_bene_mbi_id
        , eligibility_flag
        , data_sharing_flag
        , row_group

)

, latest_span_record as (

    select *
    from (
        select
              add_row_num.*
            , calculate_groups.row_group
            , row_number() over (
                partition by
                      add_row_num.current_bene_mbi_id
                    , add_row_num.eligibility_flag
                    , add_row_num.data_sharing_flag
                    , calculate_groups.row_group
                order by
                      add_row_num.coverage_month desc
                    , coalesce(add_row_num.enrollment_file_date, add_row_num.demographics_file_date) desc
              ) as span_row_num
        from add_row_num
        inner join calculate_groups
            on add_row_num.current_bene_mbi_id = calculate_groups.current_bene_mbi_id
            and add_row_num.coverage_month = calculate_groups.coverage_month
    ) ranked
    where span_row_num = 1

)

select
      cast(latest_span_record.current_bene_mbi_id as {{ dbt.type_string() }}) as person_id
    , cast(latest_span_record.current_bene_mbi_id as {{ dbt.type_string() }}) as member_id
    , cast(null as {{ dbt.type_string() }}) as subscriber_id
    , coalesce(
        case latest_span_record.enrollment_bene_sex_cd
            when '0' then 'unknown'
            when '1' then 'male'
            when '2' then 'female'
          end,
        case latest_span_record.demographics_bene_sex_cd
            when '0' then 'unknown'
            when '1' then 'male'
            when '2' then 'female'
          end
      ) as gender
    , coalesce(
        case latest_span_record.enrollment_bene_race_cd
            when '0' then 'unknown'
            when '1' then 'white'
            when '2' then 'black'
            when '3' then 'other'
            when '4' then 'asian'
            when '5' then 'hispanic'
            when '6' then 'north american native'
          end,
        case latest_span_record.demographics_bene_race_cd
            when '0' then 'unknown'
            when '1' then 'white'
            when '2' then 'black'
            when '3' then 'other'
            when '4' then 'asian'
            when '5' then 'hispanic'
            when '6' then 'north american native'
          end
      ) as race
    , coalesce(cast(latest_span_record.bene_birth_date as date), cast(latest_span_record.bene_dob as date)) as birth_date
    , coalesce(cast(latest_span_record.bene_death_date as date), cast(latest_span_record.bene_death_dt as date)) as death_date
    , cast(case
           when coalesce(latest_span_record.bene_death_date, latest_span_record.bene_death_dt) is null then 0
           else 1
      end as integer) as death_flag
    , {{ extract_year('rollup_groups.enrollment_start_date') }} as reference_year
    , cast(rollup_groups.enrollment_start_date as date) as enrollment_start_date
    , case
        when rollup_groups.enrollment_end_date >= cast({{ dbt.current_timestamp() }} as date)
        then {{ last_day(dbt.current_timestamp(), 'month') }}
        when rollup_groups.enrollment_end_date is null then {{ last_day(dbt.current_timestamp(), 'month') }}
        else cast(rollup_groups.enrollment_end_date as date)
      end as enrollment_end_date
    , cast('medicare' as {{ dbt.type_string() }}) as payer
    , cast('medicare' as {{ dbt.type_string() }}) as payer_type
    , cast('medicare' as {{ dbt.type_string() }}) as {{ quote_column('plan') }}
    , cast(latest_span_record.bene_orgnl_entlmt_rsn_cd as {{ dbt.type_string() }}) as original_reason_entitlement_code
    , cast(coalesce(latest_span_record.bene_psnyrs_dual, latest_span_record.bene_dual_stus_cd) as {{ dbt.type_string() }}) as dual_status_code
    , cast(latest_span_record.bene_mdcr_stus_cd as {{ dbt.type_string() }}) as medicare_status_code
    , cast(null as {{ dbt.type_string() }}) as enrollment_status
    , cast(null as {{ dbt.type_string() }}) as hospice_flag
    , cast(null as {{ dbt.type_string() }}) as institutional_snp_flag
    , cast(latest_span_record.lti_status as {{ dbt.type_string() }}) as long_term_institutional_flag
    , cast(null as {{ dbt.type_string() }}) as group_id
    , cast(null as {{ dbt.type_string() }}) as group_name
    , cast(latest_span_record.bene_entlmt_buyin_ind as {{ dbt.type_string() }}) as medicare_entitlement_buyin_indicator
    , cast(null as {{ dbt.type_string() }}) as name_suffix
    , cast(coalesce(latest_span_record.bene_first_name, latest_span_record.bene_1st_name) as {{ dbt.type_string() }}) as first_name
    , cast(latest_span_record.bene_midl_name as {{ dbt.type_string() }}) as middle_name
    , cast(coalesce(latest_span_record.bene_last_name, latest_span_record.demographics_bene_last_name) as {{ dbt.type_string() }}) as last_name
    , cast(null as {{ dbt.type_string() }}) as social_security_number
    , cast('self' as {{ dbt.type_string() }}) as subscriber_relation
    , {{ dbt.concat(
        [
            "latest_span_record.bene_line_1_adr",
            "latest_span_record.bene_line_2_adr",
            "latest_span_record.bene_line_3_adr",
            "latest_span_record.bene_line_4_adr",
            "latest_span_record.bene_line_5_adr",
            "latest_span_record.bene_line_6_adr"
        ]
      ) }} as address
    , cast(coalesce(latest_span_record.geo_zip_plc_name, latest_span_record.geo_ssa_cnty_cd_name) as {{ dbt.type_string() }}) as city
    , cast(coalesce(latest_span_record.bene_fips_state_cd, latest_span_record.geo_ssa_state_name) as {{ dbt.type_string() }}) as state
    , {{ dbt.concat(
        [
            "latest_span_record.geo_zip5_cd",
            "latest_span_record.geo_zip4_cd"
        ]
      ) }} as zip_code
    , cast(null as {{ dbt.type_string() }}) as phone
    , cast(null as {{ dbt.type_string() }}) as email
    , cast(null as {{ dbt.type_string() }}) as ethnicity
    , cast('medicare' as {{ dbt.type_string() }}) as data_source
    , cast(coalesce(latest_span_record.enrollment_file_name, latest_span_record.demographics_file_name) as {{ dbt.type_string() }}) as file_name
    , cast(coalesce(latest_span_record.enrollment_file_date, latest_span_record.demographics_file_date) as date) as file_date
    , cast(coalesce(latest_span_record.enrollment_file_date, latest_span_record.demographics_file_date) as {{ dbt.type_timestamp() }}) as ingest_datetime
    , cast(latest_span_record.eligibility_flag as integer) as eligibility_flag
    , cast(latest_span_record.data_sharing_flag as integer) as data_sharing_flag
    , cast(case
        when latest_span_record.inferred_eligibility_flag = 1 then 'medicare cclf (extended from alr)'
        when latest_span_record.eligibility_flag = 1 and latest_span_record.data_sharing_flag = 1 then 'cms alr connector + medicare cclf'
        when latest_span_record.eligibility_flag = 1 then 'cms alr connector'
        else 'medicare cclf'
      end as {{ dbt.type_string() }}) as x_file_type
    , cast(latest_span_record.eligibility_flag as integer) as x_eligibility_flag
    , cast(latest_span_record.data_sharing_flag as integer) as x_data_sharing_flag
    , cast(case
        when latest_span_record.inferred_eligibility_flag = 1 then 'cclf_extended_from_alr'
        when latest_span_record.eligibility_flag = 1 and latest_span_record.data_sharing_flag = 1 then 'alr_and_cclf'
        when latest_span_record.eligibility_flag = 1 then 'alr_only'
        else 'cclf_data_sharing_only'
      end as {{ dbt.type_string() }}) as x_eligibility_source
from latest_span_record
inner join rollup_groups
    on latest_span_record.current_bene_mbi_id = rollup_groups.current_bene_mbi_id
    and latest_span_record.eligibility_flag = rollup_groups.eligibility_flag
    and latest_span_record.data_sharing_flag = rollup_groups.data_sharing_flag
    and latest_span_record.row_group = rollup_groups.row_group

{% else %}

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
        row_number() over (partition by enrollment.current_bene_mbi_id, enrollment.enrollment_start_date order by enrollment.enrollment_end_date desc) as row_num
        , cast(enrollment.current_bene_mbi_id as {{ dbt.type_string() }} ) as person_id
        , cast(enrollment.current_bene_mbi_id as {{ dbt.type_string() }} ) as member_id
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
        , cast(1 as integer) as eligibility_flag
        , cast(1 as integer) as data_sharing_flag
        , cast('cclf' as {{ dbt.type_string() }}) as eligibility_source
        , 'medicare' as payer
        , 'medicare' as payer_type
        , 'medicare' as {{ quote_column('plan') }}
        , cast(demographics.bene_orgnl_entlmt_rsn_cd as {{ dbt.type_string() }} ) as original_reason_entitlement_code
        , cast(demographics.bene_dual_stus_cd as {{ dbt.type_string() }} ) as dual_status_code
        , cast(demographics.bene_mdcr_stus_cd as {{ dbt.type_string() }} ) as medicare_status_code
        , cast(null as {{ dbt.type_string() }} ) as enrollment_status
        , cast(null as {{ dbt.type_string() }} ) as hospice_flag
        , cast(null as {{ dbt.type_string() }} ) as institutional_snp_flag
        , cast(null as {{ dbt.type_string() }} ) as long_term_institutional_flag
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
        , cast(demographics.file_date as date ) as file_date
        , cast(demographics.file_date as {{ dbt.type_timestamp() }} ) as ingest_datetime
    from enrollment
    left join demographics
        on demographics.current_bene_mbi_id = enrollment.current_bene_mbi_id
        and demographics.coverage_month = {{ date_from_parts('year(enrollment.enrollment_end_date)', 'month(enrollment.enrollment_end_date)', 1) }}
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
    , {{ quote_column('plan') }}
    , original_reason_entitlement_code
    , dual_status_code
    , medicare_status_code
    , enrollment_status
    , hospice_flag
    , institutional_snp_flag
    , long_term_institutional_flag
    , null as medicaid_indicator
    , null as part_d_raf_type
    , null as low_income_subsidy_indicator
    , null as metal_level
    , null as csr_indicator
    , null as enrollment_duration_months
    , null as esrd_status
    , null as transplant_duration_months
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
    , 'medicare' as data_source
    , file_name
    , file_date
    , ingest_datetime
    , eligibility_flag
    , data_sharing_flag
    , data_source as x_file_type
    , eligibility_flag as x_eligibility_flag
    , data_sharing_flag as x_data_sharing_flag
    , eligibility_source as x_eligibility_source
from joined
WHERE row_num = 1

{% endif %}
