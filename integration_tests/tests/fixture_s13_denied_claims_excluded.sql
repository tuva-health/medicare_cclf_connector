{{ config(tags=['fixture']) }}

/*
    Fixture scenario S13: denied claims (CCLF IP v43 3.2, Dropping Denied
    Claims). Part A claims are denied in their entirety when
    CLM_MDCR_NPMT_RSN_CD is filled. Part B physician and DME claims are dropped
    on CLM_CARR_PMT_DNL_CD, and their line items on CLM_PRCSG_IND_CD.

        0000000001043  Part A, CLM_MDCR_NPMT_RSN_CD = 'N'
        0000000001044  Part B physician, CLM_PRCSG_IND_CD = 'C', CLM_CARR_PMT_DNL_CD = '0'
        0000000001045  Part B physician, CLM_PRCSG_IND_CD = 'N'
        0000000001046  Part B DME, CLM_PRCSG_IND_CD = 'N', CLM_CARR_PMT_DNL_CD = '0'

    None of them may reach the claims output.
*/

select
      claim_id
    , claim_line_number
    , claim_type
from {{ ref('medical_claim') }}
where person_id = '9TT0FK0XX13'
