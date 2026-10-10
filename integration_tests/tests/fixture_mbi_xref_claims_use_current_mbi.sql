{{ config(tags=['fixture']) }}

/*
    Fixture scenarios S14-S19: MBI history in the beneficiary XREF file
    (CCLF9). Per CCLF IP v43 5.1.1 every previous MBI is replaced with the most
    recent MBI before related claims are linked, so each claim belongs to the
    beneficiary's current MBI, and related claims filed under different MBIs
    still net against each other.

    S14  MBI changed: original under the previous MBI (first delivery),
         cancel + adjustment under the current MBI; the xref pair first
         appears in the second delivery. Part A 0000000001049 (260.00) and
         Part B 0000000001052 (101.40) are the only final-action claims.
    S15  two-hop chain (A -> B, B -> C); claims carry A and B.
    S16  xref row whose previous and current MBI are equal (obsolete date
         9999-12-31): the MBI is unchanged and the claim is not duplicated.
    S17  a previous MBI maps to one current MBI in the first two deliveries
         and to another in the later ones: the latest delivery wins.
    S18  one current MBI with two previous MBIs; claims under all three.
    S19  an xref pair present only in the earlier deliveries still applies.

    No claim may be left under a previous MBI (9TT0FK0XX80-9TT0FK0XX87).
    A null expected paid_amount means the amount is not checked.
*/

with expected as (

    select
          cast('9TT0FK0XX14' as {{ dbt.type_string() }}) as person_id
        , cast('0000000001049' as {{ dbt.type_string() }}) as claim_id
        , cast(260.00 as decimal(18, 2)) as paid_amount
    union all
    select '9TT0FK0XX14', '0000000001052', cast(101.40 as decimal(18, 2))
    union all
    select '9TT0FK0XX15', '0000000001053', cast(null as decimal(18, 2))
    union all
    select '9TT0FK0XX15', '0000000001054', cast(null as decimal(18, 2))
    union all
    select '9TT0FK0XX15', '0000000001055', cast(null as decimal(18, 2))
    union all
    select '9TT0FK0XX16', '0000000001056', cast(120.50 as decimal(18, 2))
    union all
    select '9TT0FK0XX17', '0000000001057', cast(null as decimal(18, 2))
    union all
    select '9TT0FK0XX18', '0000000001058', cast(null as decimal(18, 2))
    union all
    select '9TT0FK0XX18', '0000000001059', cast(null as decimal(18, 2))
    union all
    select '9TT0FK0XX18', '0000000001060', cast(null as decimal(18, 2))
    union all
    select '9TT0FK0XX19', '0000000001061', cast(null as decimal(18, 2))

)

, all_claims as (

    select
          person_id
        , claim_id
        , paid_amount
    from {{ ref('medical_claim') }}
    union all
    select
          person_id
        , claim_id
        , paid_amount
    from {{ ref('pharmacy_claim') }}

)

, actual as (

    select
          person_id
        , claim_id
        , sum(paid_amount) as paid_amount
        , count(*) as line_count
    from all_claims
    where person_id in (
          '9TT0FK0XX14', '9TT0FK0XX15', '9TT0FK0XX16'
        , '9TT0FK0XX17', '9TT0FK0XX18', '9TT0FK0XX19'
        , '9TT0FK0XX80', '9TT0FK0XX81', '9TT0FK0XX82', '9TT0FK0XX83'
        , '9TT0FK0XX84', '9TT0FK0XX85', '9TT0FK0XX86', '9TT0FK0XX87'
    )
       or claim_id in (select claim_id from expected)
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
        when actual.claim_id is null then 'claim missing under the current MBI'
        when expected.claim_id is null then 'claim under a previous MBI, or should have been canceled'
        else 'paid amount differs'
      end as failure
from expected
full outer join actual
    on expected.person_id = actual.person_id
   and expected.claim_id = actual.claim_id
where expected.claim_id is null
   or actual.claim_id is null
   or expected.paid_amount <> actual.paid_amount
