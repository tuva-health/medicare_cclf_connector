{{ config(tags=['fixture', 'tuva-94']) }}

/*
    Fixture scenario S29 (TUVA-94): a Part D original followed by two
    adjustments (0,2,2). Both adjustments carry CLM_EFCTV_DT = 1000-01-01, a
    missing date (CCLF IP v43 3.6), and arrive in different deliveries. The
    dispensing status is blank on all three.

    Per IP 3.3, when monthly Part D files are combined, the most recent claim
    in a related set is the final-action claim and the earlier ones are
    ignored. The adjustment in the later delivery, 0000000001085
    (2026-02-09), supersedes the earlier one, 0000000001084 (2026-01-12), and
    exactly one claim remains. Sorting on adjustment type alone ties the two
    adjustments.
*/

with actual as (

    select
          claim_id
        , count(*) as line_count
    from {{ ref('pharmacy_claim') }}
    where claim_id in ('0000000001083', '0000000001084', '0000000001085')
    group by claim_id

)

select
      coalesce(expected.claim_id, actual.claim_id) as claim_id
    , actual.line_count
from (select cast('0000000001085' as {{ dbt.type_string() }}) as claim_id) as expected
full outer join actual
    on expected.claim_id = actual.claim_id
where expected.claim_id is null
   or actual.claim_id is null
   or actual.line_count <> 1
