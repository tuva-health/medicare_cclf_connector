{{ config(tags=['fixture']) }}

/*
    Fixture scenario S12: the same claim version is delivered in two monthly
    files. A re-delivered copy is the same claim (same CUR_CLM_UNIQ_ID,
    adjustment type and effective date), so it must be counted once, from the
    latest delivery, even when a non-key column differs between copies.

        0000000001040  Part A, identical copies (2025-11-10, 2025-12-08)
        0000000001041  Part A, BENE_HIC_NUM differs (2025-12-08, 2026-01-12)
        0000000001042  Part B, HCPCS_BETOS_CD blanked (2025-11-10, 2025-12-08)

    Part A claims carry their delivery date in ingest_datetime only.
*/

with expected as (

    select
          cast('0000000001040' as {{ dbt.type_string() }}) as claim_id
        , 4 as line_count
        , cast(133.20 as decimal(18, 2)) as paid_amount
        , cast('2025-12-08' as {{ dbt.type_string() }}) as delivery_date
    union all
    select '0000000001041', 4, cast(61.15 as decimal(18, 2)), '2026-01-12'
    union all
    select '0000000001042', 2, cast(76.67 as decimal(18, 2)), '2025-12-08'

)

, actual as (

    select
          claim_id
        , count(*) as line_count
        , count(distinct claim_line_number) as distinct_line_count
        , sum(paid_amount) as paid_amount
        , min(left(cast(ingest_datetime as {{ dbt.type_string() }}), 10)) as min_delivery_date
        , max(left(cast(ingest_datetime as {{ dbt.type_string() }}), 10)) as max_delivery_date
    from {{ ref('medical_claim') }}
    where person_id = '9TT0FK0XX12'
    group by claim_id

)

select
      coalesce(expected.claim_id, actual.claim_id) as claim_id
    , expected.line_count as expected_line_count
    , actual.line_count as actual_line_count
    , expected.paid_amount as expected_paid_amount
    , actual.paid_amount as actual_paid_amount
    , expected.delivery_date as expected_delivery_date
    , actual.max_delivery_date as actual_delivery_date
from expected
full outer join actual
    on expected.claim_id = actual.claim_id
where expected.claim_id is null
   or actual.claim_id is null
   or actual.line_count <> expected.line_count
   or actual.distinct_line_count <> expected.line_count
   or actual.paid_amount <> expected.paid_amount
   or actual.min_delivery_date <> expected.delivery_date
   or actual.max_delivery_date <> expected.delivery_date
