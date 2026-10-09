{{ config(tags=['fixture']) }}

/*
    Fixture scenarios S02, S05, S07, S08, S09, S10 and S11: Part A related
    claims (CCLF IP v43 5.1.2 natural key, 5.2.1 patterns). Every cancellation
    is matched with an original or adjustment claim and the matched pairs are
    removed, leaving only the final-action claims. Each beneficiary below must
    have exactly the expected final-action claims and no others.

    S02  original, then cancel + adjustment on one effective date, in a later
         delivery: the adjustment is final; net paid 325.50.
    S05  two originals and nothing else (pattern 4): both are final.
    S07  original + cancellation, no replacement: no claim.
    S08  cancel + adjustment whose original predates the files: the adjustment
         is final; a separate cancellation-only set yields no claim. The paid
         amount is not checked here, because the related set's debit/credit
         total (250.00) and the claim's own payment (6650.00) differ when the
         original is missing.
    S09  adjustment with no other related claims (pattern 2): it is final.
    S10  through date corrected (5.2.1): the cancellation keeps the old
         through date, so the old key nets to zero; the adjustment is final
         under the new key.
    S11  chain 0,1,2,1,2 over three deliveries: the last adjustment is final.

    A null expected paid_amount means the amount is not checked.
*/

with expected as (

    select
          cast('9TT0FK0XX02' as {{ dbt.type_string() }}) as person_id
        , cast('0000000001010' as {{ dbt.type_string() }}) as claim_id
        , cast(325.50 as decimal(18, 2)) as paid_amount
    union all
    select '9TT0FK0XX05', '0000000001021', cast(9850.10 as decimal(18, 2))
    union all
    select '9TT0FK0XX05', '0000000001022', cast(1215.40 as decimal(18, 2))
    union all
    select '9TT0FK0XX08', '0000000001029', cast(null as decimal(18, 2))
    union all
    select '9TT0FK0XX09', '0000000001031', cast(2210.75 as decimal(18, 2))
    union all
    select '9TT0FK0XX10', '0000000001034', cast(7480.00 as decimal(18, 2))
    union all
    select '9TT0FK0XX11', '0000000001039', cast(505.00 as decimal(18, 2))

)

, actual as (

    select
          person_id
        , claim_id
        , sum(paid_amount) as paid_amount
    from {{ ref('medical_claim') }}
    where person_id in (
          '9TT0FK0XX02'
        , '9TT0FK0XX05'
        , '9TT0FK0XX07'
        , '9TT0FK0XX08'
        , '9TT0FK0XX09'
        , '9TT0FK0XX10'
        , '9TT0FK0XX11'
    )
    group by
          person_id
        , claim_id

)

select
      coalesce(expected.person_id, actual.person_id) as person_id
    , coalesce(expected.claim_id, actual.claim_id) as claim_id
    , expected.paid_amount as expected_paid_amount
    , actual.paid_amount as actual_paid_amount
    , case
        when actual.claim_id is null then 'expected final-action claim missing'
        when expected.claim_id is null then 'claim should have been canceled or superseded'
        else 'paid amount differs'
      end as failure
from expected
full outer join actual
    on expected.person_id = actual.person_id
   and expected.claim_id = actual.claim_id
where expected.claim_id is null
   or actual.claim_id is null
   or expected.paid_amount <> actual.paid_amount
