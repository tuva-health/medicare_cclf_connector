{{ config(tags=['fixture', 'tuva-94']) }}

/*
    Fixture scenario S04 (TUVA-94): a replica of the CCLF IP v43 5.3.2 worked
    example (Table 4). Claims #1-#6 are one related set, #7 is a separate claim.

        #1 0000000001014  $200  original      07/20
        #2 0000000001015    $0  adjustment    09/20
        #3 0000000001016  $200  cancellation  08/07  (cancels #1)
        #4 0000000001017  $210  adjustment    09/20
        #5 0000000001018    $0  adjustment    08/07
        #6 0000000001019    $0  cancellation  10/07  (cancels #2 or #5)
        #7 0000000001020   $50  original      09/20  (separate set)

    The IP's final-action claims are #4, #7 and either #2 or #5, but not both.
    The beneficiary's Part A total is $200 + $0 - $200 + $210 + $0 - $0 + $50
    = $260, which is also the sum of the final-action claims' payments.
*/

with actual as (

    select
          claim_id
        , sum(paid_amount) as paid_amount
    from {{ ref('medical_claim') }}
    where person_id = '9TT0FK0XX04'
    group by claim_id

)

, summary as (

    select
          sum(case when claim_id = '0000000001017' then 1 else 0 end) as claim_4_count
        , sum(case when claim_id = '0000000001020' then 1 else 0 end) as claim_7_count
        , sum(case when claim_id in ('0000000001015', '0000000001018') then 1 else 0 end) as claim_2_or_5_count
        , sum(case
                when claim_id not in ('0000000001017', '0000000001020', '0000000001015', '0000000001018') then 1
                else 0
              end) as unexpected_count
        , sum(paid_amount) as total_paid_amount
    from actual

)

select
      claim_4_count
    , claim_7_count
    , claim_2_or_5_count
    , unexpected_count
    , total_paid_amount
from summary
where coalesce(claim_4_count, 0) <> 1
   or coalesce(claim_7_count, 0) <> 1
   or coalesce(claim_2_or_5_count, 0) <> 1
   or coalesce(unexpected_count, 0) <> 0
   or coalesce(total_paid_amount, 0) <> cast(260.00 as decimal(18, 2))
