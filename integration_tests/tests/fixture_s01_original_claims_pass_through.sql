{{ config(tags=['fixture']) }}

/*
    Fixture scenario S01: one original claim of every claim type and no related
    claims (CCLF IP v43 5.2.1 pattern 1). Each claim is its own final-action
    claim, so it must reach the output once, with its paid amount unchanged.
    The Part B claim keeps its $0 line, and the inpatient claim carries its
    CCLF3 procedures.
*/

with expected as (

    select cast('0000000001001' as {{ dbt.type_string() }}) as claim_id, 4 as line_count, cast(11876.42 as decimal(18, 2)) as paid_amount
    union all
    select cast('0000000001002' as {{ dbt.type_string() }}), 4, cast(412.18 as decimal(18, 2))
    union all
    select cast('0000000001003' as {{ dbt.type_string() }}), 4, cast(7210.00 as decimal(18, 2))
    union all
    select cast('0000000001004' as {{ dbt.type_string() }}), 4, cast(1984.55 as decimal(18, 2))
    union all
    select cast('0000000001005' as {{ dbt.type_string() }}), 3, cast(111.95 as decimal(18, 2))
    union all
    select cast('0000000001006' as {{ dbt.type_string() }}), 2, cast(71.30 as decimal(18, 2))

)

, medical as (

    select
          claim_id
        , count(*) as line_count
        , count(distinct claim_line_number) as distinct_line_count
        , sum(paid_amount) as paid_amount
        , max(procedure_code_1) as procedure_code_1
    from {{ ref('medical_claim') }}
    where person_id = '9TT0FK0XX01'
    group by claim_id

)

, pharmacy as (

    select
          claim_id
        , count(*) as line_count
    from {{ ref('pharmacy_claim') }}
    where person_id = '9TT0FK0XX01'
    group by claim_id

)

, medical_failures as (

    select
          coalesce(expected.claim_id, medical.claim_id) as claim_id
        , 'medical claim missing, unexpected, duplicated or paid amount changed' as failure
    from expected
    full outer join medical
        on expected.claim_id = medical.claim_id
    where expected.claim_id is null
       or medical.claim_id is null
       or medical.line_count <> expected.line_count
       or medical.distinct_line_count <> expected.line_count
       or medical.paid_amount <> expected.paid_amount

)

, procedure_failures as (

    select
          claim_id
        , 'inpatient claim lost its CCLF3 procedures' as failure
    from medical
    where claim_id = '0000000001001'
      and procedure_code_1 is null

)

, pharmacy_failures as (

    select
          cast('0000000001007' as {{ dbt.type_string() }}) as claim_id
        , 'Part D claim missing, unexpected or duplicated' as failure
    from (
        select
              sum(case when claim_id = '0000000001007' then line_count else 0 end) as expected_lines
            , sum(case when claim_id <> '0000000001007' then line_count else 0 end) as other_lines
        from pharmacy
    ) as pharmacy_summary
    where coalesce(expected_lines, 0) <> 1
       or coalesce(other_lines, 0) <> 0

)

select * from medical_failures
union all
select * from procedure_failures
union all
select * from pharmacy_failures
