{{ config(tags=['fixture', 'tuva-94']) }}

/*
    Fixture scenario S03 (TUVA-94): original, then a cancellation and an
    adjustment processed together, so they share CLM_EFCTV_DT. The
    cancellation carries the higher CUR_CLM_UNIQ_ID.

    Per CCLF IP v43 5.2.1 the cancellation is matched with the original and
    the pair is removed, so the adjustment 0000000001012 is the final-action
    claim, with net paid 210.00 - 210.00 + 190.00 = 190.00. Claim ID order
    carries no meaning, so it must not decide the winner.
*/

with expected as (

    select
          cast('0000000001012' as {{ dbt.type_string() }}) as claim_id
        , cast(190.00 as decimal(18, 2)) as paid_amount

)

, actual as (

    select
          claim_id
        , sum(paid_amount) as paid_amount
    from {{ ref('medical_claim') }}
    where person_id = '9TT0FK0XX03'
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
