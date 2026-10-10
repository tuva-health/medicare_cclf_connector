{{ config(tags=['fixture']) }}

/*
    Fixture scenarios S22-S25: Part B physician (CCLF5) and DME (CCLF6)
    related claims, linked on CLM_CNTL_NUM and the most recent MBI (CCLF IP
    v43 5.1.2). Cancellations are matched with the claims they cancel and the
    pairs are removed (5.2.2, 5.2.3). Each beneficiary must have exactly these
    final-action claim lines.

    S22  replica of the IP 5.3.2 Table 6 example: claims #3 (original), #2
         (original) and #1 (cancellation of #3) on one control number. #2,
         0000000001066, is final; its lines sum to the example's $707.
    S23  Part B original, then cancellation + adjustment (adjustment type 2
         does occur in CCLF5): the adjustment 0000000001070 is final, with
         line 1 re-coded to 99215 and paid 135.60.
    S24  Part B original + cancellation, no replacement: no lines remain.
    S25  DME 0,1,2 with the line 2 amount reduced: 0000000001075 is final
         (71.30 + 18.40); a separate DME 0,1 set nets to nothing.
*/

with expected as (

    select
          cast('9TT0FK0XX23' as {{ dbt.type_string() }}) as person_id
        , cast('0000000001066' as {{ dbt.type_string() }}) as claim_id
        , 1 as claim_line_number
        , cast('99214' as {{ dbt.type_string() }}) as hcpcs_code
        , cast(380.00 as decimal(18, 2)) as paid_amount
    union all
    select '9TT0FK0XX23', '0000000001066', 2, '93000', cast(227.00 as decimal(18, 2))
    union all
    select '9TT0FK0XX23', '0000000001066', 3, '80053', cast(100.00 as decimal(18, 2))
    union all
    select '9TT0FK0XX24', '0000000001070', 1, '99215', cast(135.60 as decimal(18, 2))
    union all
    select '9TT0FK0XX24', '0000000001070', 2, '93000', cast(13.74 as decimal(18, 2))
    union all
    select '9TT0FK0XX26', '0000000001075', 1, 'E0601', cast(71.30 as decimal(18, 2))
    union all
    select '9TT0FK0XX26', '0000000001075', 2, 'A7035', cast(18.40 as decimal(18, 2))

)

, actual as (

    select
          person_id
        , claim_id
        , claim_line_number
        , hcpcs_code
        , paid_amount
    from {{ ref('medical_claim') }}
    where person_id in ('9TT0FK0XX23', '9TT0FK0XX24', '9TT0FK0XX25', '9TT0FK0XX26')

)

select
      coalesce(expected.person_id, actual.person_id) as person_id
    , coalesce(expected.claim_id, actual.claim_id) as claim_id
    , coalesce(expected.claim_line_number, actual.claim_line_number) as claim_line_number
    , expected.hcpcs_code as expected_hcpcs_code
    , actual.hcpcs_code as actual_hcpcs_code
    , expected.paid_amount as expected_paid_amount
    , actual.paid_amount as actual_paid_amount
from expected
full outer join actual
    on expected.person_id = actual.person_id
   and expected.claim_id = actual.claim_id
   and expected.claim_line_number = actual.claim_line_number
where expected.claim_id is null
   or actual.claim_id is null
   or coalesce(actual.hcpcs_code, '') <> expected.hcpcs_code
   or actual.paid_amount <> expected.paid_amount
