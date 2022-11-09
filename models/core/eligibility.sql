/*
  This model takes in eligibility data on the member month grain and converts
  it to enrollment date spans using row number and lag to account for
  continuous enrollment and gaps in coverage.
*/

with demographics as (

    select * from {{ var('beneficiary_demographics') }}

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
        , cast(bene_member_month as date) as bene_member_month
        , row_num
        , lag(cast(bene_member_month as date)) over (
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
        , datediff(month, lag_enrollment, bene_member_month) as lag_diff
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
        , last_day(max(bene_member_month)) as enrollment_end_date
    from calculate_groups
    group by bene_mbi_id, row_group

),

joined as (

    select
          cast(enrollment_span.bene_mbi_id as varchar) as patient_id
        , cast(NULL as varchar) as member_id
        , cast(case demographics.bene_sex_cd
               when '0' then 'unknown'
               when '1' then 'male'
               when '2' then 'female'
          end as varchar) as gender
        , cast(case demographics.bene_race_cd
               when '0' then 'unknown'
               when '1' then 'white'
               when '2' then 'black'
               when '3' then 'other'
               when '4' then 'asian'
               when '5' then 'hispanic'
               when '6' then 'north american native'
          end as varchar) as race
        , cast(demographics.bene_dob as date) as birth_date
        , cast(demographics.bene_death_dt as date) as death_date
        , cast(case
               when demographics.bene_death_dt is null then 0
               else 1
          end as int) as death_flag
        , enrollment_span.enrollment_start_date
        , enrollment_span.enrollment_end_date
        , cast('medicare' as varchar) as payer
        , cast('medicare' as varchar) as payer_type
        , cast(demographics.bene_dual_stus_cd as varchar) as dual_status_code
        , cast(demographics.bene_mdcr_stus_cd as varchar) as medicare_status_code
        , cast(demographics.bene_1st_name as varchar) as first_name
        , cast(demographics.bene_last_name as varchar) as last_name
        , cast(demographics.bene_line_1_adr as varchar) as address
        , cast(demographics.geo_zip_plc_name as varchar) as city
        , cast(medicare_state_fips.state as varchar) as state
        , cast(demographics.bene_zip_cd as varchar) as zip_code
        , cast(NULL as varchar) as phone
        , cast('cclf' as varchar) as data_source
    from enrollment_span
         left join demographics
            on enrollment_span.bene_mbi_id = demographics.bene_mbi_id
            and enrollment_span.enrollment_end_date = demographics.bene_member_month
         left join medicare_state_fips
            on demographics.bene_fips_state_cd = medicare_state_fips.fips_code

)

select * from joined