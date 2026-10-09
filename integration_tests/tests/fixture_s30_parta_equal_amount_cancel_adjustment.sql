{{ config(tags=['fixture', 'tuva-94']) }}

/*
    Fixture scenario S30 (TUVA-94): a cancellation and an adjustment with the
    same CLM_PMT_AMT (420.00), issued in the same action (same CLM_EFCTV_DT and
    delivery). The original they replace predates the files.

    The cancellation targets the missing original. The adjustment issued
    alongside it does not precede it, so it is not the cancellation's partner
    (CCLF IP v43 5.2.1: a cancellation is matched with the claim it cancels).
    The adjustment 0000000001087 is the final-action claim and carries its own
    payment, 420.00.
*/

with expected as (

    select
          cast('0000000001087' as {{ dbt.type_string() }}) as claim_id
        , cast(420.00 as decimal(18, 2)) as paid_amount

)

, actual as (

    select
          claim_id
        , sum(paid_amount) as paid_amount
    from {{ ref('medical_claim') }}
    where person_id = '9TT0FK0XX28'
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
