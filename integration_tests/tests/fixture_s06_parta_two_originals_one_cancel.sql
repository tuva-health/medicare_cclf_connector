{{ config(tags=['fixture', 'tuva-94']) }}

/*
    Fixture scenario S06 (TUVA-94): two original claims and one cancellation
    in a single related set, with the cancellation as the latest version.

    Per CCLF IP v43 5.2.1 the cancellation 0000000001025 is matched with the
    original it cancels, 0000000001023, and the pair is removed. The other
    original, 0000000001024, remains a final-action claim ("it is possible
    that there is more than one final action claim among a related set"),
    with net paid 150.00 + 95.00 - 150.00 = 95.00.
*/

with expected as (

    select
          cast('0000000001024' as {{ dbt.type_string() }}) as claim_id
        , cast(95.00 as decimal(18, 2)) as paid_amount

)

, actual as (

    select
          claim_id
        , sum(paid_amount) as paid_amount
    from {{ ref('medical_claim') }}
    where person_id = '9TT0FK0XX06'
    group by claim_id

)

select
      coalesce(expected.claim_id, actual.claim_id) as claim_id
    , expected.paid_amount as expected_paid_amount
    , actual.paid_amount as actual_paid_amount
from expected
full outer join actual
    on expected.claim_id = actual.claim_id
where expected.claim_id is null
   or actual.claim_id is null
   or expected.paid_amount <> actual.paid_amount
