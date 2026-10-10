{{ config(tags=['fixture']) }}

/*
    Fixture scenarios S26-S28: Part D (CCLF7) related claims, linked on the
    Part D natural key (CCLF IP v43 5.1.2: fill date, service provider,
    dispensing status, Rx service reference number and fill number). Most
    Part D rows carry CLM_EFCTV_DT = 1000-01-01, a missing date (IP 3.6).

    S26  original + adjustment, no cancellation (0,2): the adjustment
         0000000001079 is final.
    S27  original + reversal (0,1): no final claim.
    S28  adjustment only, with a real effective date: 0000000001082 is kept.

    Beneficiary 9TT0FK0XX27 also holds S29's claims, which are checked in
    fixture_s29_partd_two_adjustments_tie; they are left out here.
*/

with expected as (

    select cast('0000000001079' as {{ dbt.type_string() }}) as claim_id
    union all
    select '0000000001082'

)

, actual as (

    select
          claim_id
        , count(*) as line_count
    from {{ ref('pharmacy_claim') }}
    where person_id = '9TT0FK0XX27'
      and claim_id not in ('0000000001083', '0000000001084', '0000000001085')
    group by claim_id

)

select
      coalesce(expected.claim_id, actual.claim_id) as claim_id
    , actual.line_count
from expected
full outer join actual
    on expected.claim_id = actual.claim_id
where expected.claim_id is null
   or actual.claim_id is null
   or actual.line_count <> 1
