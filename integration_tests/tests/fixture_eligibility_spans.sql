{{ config(tags=['fixture']) }}

/*
    Fixture scenarios S16, S21 and S21b: eligibility built from the enrollment
    source and the beneficiary demographics file (CCLF8). The checks do not
    depend on whether enrollment is stored as monthly rows or merged spans.

    S16   a self-mapping XREF row must not duplicate the person: no two
          eligibility rows for 9TT0FK0XX16 start on the same date.
    S21   enrollment gap April-June 2025: 9TT0FK0XX21 is covered 2025-01 to
          2025-03 and 2025-07 to 2026-02, and nothing overlaps the gap.
    S21b  beneficiary first seen in the CY26 CCLF8: 9TT0FK0XX22 is covered
          from 2026-01, and its latest row carries dual status 02.
*/

with eligibility as (

    select
          person_id
        , enrollment_start_date
        , enrollment_end_date
        , dual_status_code
    from {{ ref('eligibility') }}
    where person_id in ('9TT0FK0XX16', '9TT0FK0XX21', '9TT0FK0XX22')

)

, s16_duplicates as (

    select
          'S16' as scenario
        , 'duplicate eligibility rows for one start date' as failure
    from eligibility
    where person_id = '9TT0FK0XX16'
    group by enrollment_start_date
    having count(*) > 1

)

, s21_summary as (

    select
          min(enrollment_start_date) as first_start_date
        , max(enrollment_end_date) as last_end_date
        , sum(case
                when enrollment_start_date <= cast('2025-06-30' as date)
                 and enrollment_end_date >= cast('2025-04-01' as date) then 1
                else 0
              end) as rows_in_gap
        , sum(case when enrollment_end_date = cast('2025-03-31' as date) then 1 else 0 end) as rows_ending_before_gap
        , sum(case when enrollment_start_date = cast('2025-07-01' as date) then 1 else 0 end) as rows_starting_after_gap
    from eligibility
    where person_id = '9TT0FK0XX21'

)

, s21_failures as (

    select
          'S21' as scenario
        , 'coverage does not match 2025-01..2025-03 and 2025-07..2026-02' as failure
    from s21_summary
    where first_start_date is null
       or first_start_date <> cast('2025-01-01' as date)
       or last_end_date <> cast('2026-02-28' as date)
       or rows_in_gap <> 0
       or rows_ending_before_gap <> 1
       or rows_starting_after_gap <> 1

)

, s21b_latest as (

    select
          enrollment_start_date
        , dual_status_code
        , row_number() over (order by enrollment_start_date desc) as row_num
        , min(enrollment_start_date) over () as first_start_date
    from eligibility
    where person_id = '9TT0FK0XX22'

)

, s21b_failures as (

    select
          'S21b' as scenario
        , 'coverage does not start 2026-01, or dual status 02 not carried' as failure
    from (select 1 as one) as anchor
    left join s21b_latest
        on s21b_latest.row_num = 1
    where s21b_latest.first_start_date is null
       or s21b_latest.first_start_date <> cast('2026-01-01' as date)
       or coalesce(s21b_latest.dual_status_code, '') <> '02'

)

select * from s16_duplicates
union all
select * from s21_failures
union all
select * from s21b_failures
