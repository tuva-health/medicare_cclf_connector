{{ config(tags=['fixture']) }}

/*
    Every fixture claim row comes from a delivered CCLF file with a FILE_DATE,
    so every medical_claim row (Part A, Part B physician and DME) and every
    pharmacy_claim row (Part D) must carry that file_date. It is the same date
    the connector publishes as ingest_datetime.
*/

select
      'medical_claim' as model_name
    , claim_id
    , claim_line_number
    , claim_type
    , file_date
from {{ ref('medical_claim') }}
where file_date is null
   or file_date <> cast(ingest_datetime as date)

union all

select
      'pharmacy_claim' as model_name
    , claim_id
    , claim_line_number
    , 'pharmacy' as claim_type
    , file_date
from {{ ref('pharmacy_claim') }}
where file_date is null
   or file_date <> cast(ingest_datetime as date)
