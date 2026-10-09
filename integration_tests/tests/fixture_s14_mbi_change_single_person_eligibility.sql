{{ config(tags=['fixture']) }}

/*
    Fixture scenario S14, eligibility: the beneficiary's MBI changed, and the
    enrollment source lists January-June 2025 under the previous MBI
    9TT0FK0XX80. CCLF9 maps it to the current MBI 9TT0FK0XX14.

    Per CCLF IP v43 5.1.1 the previous MBI is replaced with the most recent
    one, so this is one person: no eligibility row may remain under
    9TT0FK0XX80, and 9TT0FK0XX14 is covered from 2025-01-01.
*/

with eligibility as (

    select
          person_id
        , enrollment_start_date
    from {{ ref('eligibility') }}
    where person_id in ('9TT0FK0XX14', '9TT0FK0XX80')

)

, previous_mbi_rows as (

    select
          person_id
        , enrollment_start_date
        , 'eligibility row left under the previous MBI' as failure
    from eligibility
    where person_id = '9TT0FK0XX80'

)

, current_mbi_coverage as (

    select
          cast('9TT0FK0XX14' as {{ dbt.type_string() }}) as person_id
        , min(enrollment_start_date) as enrollment_start_date
        , 'current MBI not covered from 2025-01-01' as failure
    from eligibility
    where person_id = '9TT0FK0XX14'
    having min(enrollment_start_date) is null
        or min(enrollment_start_date) <> cast('2025-01-01' as date)

)

select * from previous_mbi_rows
union all
select * from current_mbi_coverage
