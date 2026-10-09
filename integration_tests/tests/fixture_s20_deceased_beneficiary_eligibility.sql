{{ config(tags=['fixture']) }}

/*
    Fixture scenario S20: beneficiary 9TT0FK0XX20 died on 2025-11-18. CCLF8
    reports BENE_DEATH_DT from the first delivery after the death, and the
    beneficiary drops out of the CY26 CCLF8 and enrollment.

    The death date must reach eligibility (on the latest coverage row at
    least), and there is no coverage after 2025-11.
*/

with eligibility as (

    select
          enrollment_start_date
        , enrollment_end_date
        , death_date
        , death_flag
        , row_number() over (order by enrollment_start_date desc) as row_num
    from {{ ref('eligibility') }}
    where person_id = '9TT0FK0XX20'

)

select
      latest.enrollment_start_date
    , latest.enrollment_end_date
    , latest.death_date
    , latest.death_flag
from (select 1 as one) as anchor
left join eligibility as latest
    on latest.row_num = 1
where latest.enrollment_end_date is null
   or latest.enrollment_end_date > cast('2025-11-30' as date)
   or latest.death_date is null
   or latest.death_date <> cast('2025-11-18' as date)
   or latest.death_flag <> 1
