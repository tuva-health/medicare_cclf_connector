/*
  This model takes in eligibility data on the member month grain and converts
  it to enrollment date spans using row number and lag to account for
  continuous enrollment and gaps in coverage.
*/

with demographics as (

    select
          bene_mbi_id
        , bene_sex_cd
        , bene_race_cd
        , bene_dob
        , bene_death_dt
        , {{ try_to_cast_date('bene_member_month', 'YYYY-MM-DD') }} as bene_member_month
        , bene_dual_stus_cd
        , bene_mdcr_stus_cd
        , bene_1st_name
        , bene_last_name
        , bene_line_1_adr
        , geo_zip_plc_name
        , bene_fips_state_cd
        , bene_zip_cd
    from {{ var('beneficiary_demographics') }}

),

medicare_state_fips as (

    select * from {{ ref('medicare_state_fips') }}

),

add_row_num as (

    select *
         , row_number() over (
             partition by bene_mbi_id
             order by bene_member_month
           ) as row_num
    from demographics

),

add_lag_enrollment as (

    select
          bene_mbi_id
        , bene_member_month
        , row_num
        , lag(bene_member_month) over (
            partition by bene_mbi_id
            order by row_num
          ) as lag_enrollment
    from add_row_num

),

calculate_lag_diff as (

    select
          bene_mbi_id
        , bene_member_month
        , row_num
        , lag_enrollment
        , {{ datediff('lag_enrollment', 'bene_member_month', 'month') }} as lag_diff
    from add_lag_enrollment

),

calculate_gaps as (

     select
          bene_mbi_id
        , bene_member_month
        , row_num
        , lag_enrollment
        , lag_diff
        , case
            when lag_diff > 1 then 1
            else 0
          end as gap_flag
    from calculate_lag_diff

),

calculate_groups as (

     select
          bene_mbi_id
        , bene_member_month
        , row_num
        , lag_enrollment
        , lag_diff
        , gap_flag
        , sum(gap_flag) over (
            partition by bene_mbi_id
            order by row_num
            rows between unbounded preceding and current row
          ) as row_group
    from calculate_gaps

),

enrollment_span as (

    select
          bene_mbi_id
        , row_group
        , min(bene_member_month) as enrollment_start_date
        , max(bene_member_month) as enrollment_end_date_max
        , last_day(max(bene_member_month)) as enrollment_end_date_last
    from calculate_groups
    group by bene_mbi_id, row_group

),

joined as (

    select
          {{ cast_string_or_varchar('enrollment_span.bene_mbi_id') }} as patient_id
        , {{ cast_string_or_varchar('NULL') }} as member_id
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
        , enrollment_span.enrollment_start_date
        , enrollment_span.enrollment_end_date_last as enrollment_end_date
        , 'medicare' as payer
        , 'medicare' as payer_type
        , {{ cast_string_or_varchar('demographics.bene_dual_stus_cd') }} as dual_status_code
        , {{ cast_string_or_varchar('demographics.bene_mdcr_stus_cd') }} as medicare_status_code
        , {{ cast_string_or_varchar('demographics.bene_1st_name') }} as first_name
        , {{ cast_string_or_varchar('demographics.bene_last_name') }} as last_name
        , {{ cast_string_or_varchar('demographics.bene_line_1_adr') }} as address
        , {{ cast_string_or_varchar('demographics.geo_zip_plc_name') }} as city
        , {{ cast_string_or_varchar('medicare_state_fips.state') }} as state
        , {{ cast_string_or_varchar('demographics.bene_zip_cd') }} as zip_code
        , {{ cast_string_or_varchar('NULL') }} as phone
        , 'cclf' as data_source
    from enrollment_span
         left join demographics
            on enrollment_span.bene_mbi_id = demographics.bene_mbi_id
            and enrollment_span.enrollment_end_date_max = demographics.bene_member_month
         left join medicare_state_fips
            on demographics.bene_fips_state_cd = medicare_state_fips.fips_code

)

select * from joined