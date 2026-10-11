{{ config(tags=['fixture']) }}

/*
    Fixture scenarios S31, S31b, S32 and S33 (TUVA-119): dual_status_code
    when BENE_DUAL_STUS_CD differs between CCLF8 deliveries (IP v43 Table 21).
    Each person is enrolled 2025-01 to 2026-02. CCLF8 is delivered in
    2025-11 (F1), 2025-12 (F2), 2026-01 (F3) and 2026-02 (F4). A row takes
    the code from the latest delivery on or before its last month whose code
    is not blank. If there is none, it takes the earliest later delivery.
    'NA' then maps to null. The checks hold whether enrollment is stored as
    monthly rows or as one merged span.

    S31   9TT0FK0XX29  02 in F1-F3, NA in F4. The row covering 2026-02 is
                       null; the NA is not skipped to revive the earlier 02.
                       Rows ending by 2026-01 keep 02.
    S31b  9TT0FK0XX30  NA in F1-F3, 02 in F4. The row covering 2026-02 is
                       02. Rows ending by 2026-01 are null.
    S32   9TT0FK0XX31  02 in F1-F3, blank in F4. Every row is 02: the blank
                       delivery is skipped, not passed through.
    S33   9TT0FK0XX32  never in CCLF8. Every row is null.
*/

with eligibility as (

    select
          person_id
        , enrollment_start_date
        , enrollment_end_date
        , dual_status_code
        , case
            when enrollment_start_date <= cast('2026-02-01' as date)
             and enrollment_end_date >= cast('2026-02-01' as date) then 1
            else 0
          end as covers_2026_02
        , case
            when enrollment_end_date <= cast('2026-01-31' as date) then 1
            else 0
          end as ends_by_2026_01
    from {{ ref('eligibility') }}
    where person_id in ('9TT0FK0XX29', '9TT0FK0XX30', '9TT0FK0XX31', '9TT0FK0XX32')

)

, failures as (

    select
          person_id
        , enrollment_start_date
        , dual_status_code
        , 'unexpected dual_status_code' as failure
    from eligibility
    where (person_id = '9TT0FK0XX29' and covers_2026_02 = 1 and dual_status_code is not null)
       or (person_id = '9TT0FK0XX29' and ends_by_2026_01 = 1 and coalesce(dual_status_code, '') <> '02')
       or (person_id = '9TT0FK0XX30' and covers_2026_02 = 1 and coalesce(dual_status_code, '') <> '02')
       or (person_id = '9TT0FK0XX30' and ends_by_2026_01 = 1 and dual_status_code is not null)
       or (person_id = '9TT0FK0XX31' and coalesce(dual_status_code, '') <> '02')
       or (person_id = '9TT0FK0XX32' and dual_status_code is not null)

)

/* every person must have a row covering 2026-02 */
, missing as (

    select
          expected.person_id
        , cast(null as date) as enrollment_start_date
        , cast(null as {{ dbt.type_string() }}) as dual_status_code
        , 'no eligibility row covering 2026-02' as failure
    from (
        select '9TT0FK0XX29' as person_id
        union all
        select '9TT0FK0XX30' as person_id
        union all
        select '9TT0FK0XX31' as person_id
        union all
        select '9TT0FK0XX32' as person_id
    ) as expected
    left join eligibility
        on eligibility.person_id = expected.person_id
        and eligibility.covers_2026_02 = 1
    where eligibility.person_id is null

)

select * from failures
union all
select * from missing
