with deduped_claims as (

    select * from {{ ref('int_pharmacy_claim_deduped') }}

)

, data_types as (

    select
          cast(claim_id as {{ dbt.type_string() }}) as claim_id
        , cast(claim_line_number as integer) as claim_line_number
        , cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(member_id as {{ dbt.type_string() }}) as member_id
        , cast(payer as {{ dbt.type_string() }}) as payer
        , cast({{ the_tuva_project.quote_column('plan') }} as {{ dbt.type_string() }}) as {{ the_tuva_project.quote_column('plan') }}
        , cast(prescribing_provider_npi as {{ dbt.type_string() }}) as prescribing_provider_npi
        , cast(dispensing_provider_npi as {{ dbt.type_string() }}) as dispensing_provider_npi
        , cast(dispensing_date as date) as dispensing_date
        , cast(ndc_code as {{ dbt.type_string() }}) as ndc_code
        , cast(quantity as integer) as quantity
        , cast(days_supply as integer) as days_supply
        , cast(refills as integer) as refills
        , cast(paid_date as date) as paid_date
        , {{ cast_numeric('paid_amount') }} as paid_amount
        , {{ cast_numeric('allowed_amount') }} as allowed_amount
        , {{ cast_numeric('charge_amount') }} as charge_amount
        , {{ cast_numeric('coinsurance_amount') }} as coinsurance_amount
        , {{ cast_numeric('copayment_amount') }} as copayment_amount
        , {{ cast_numeric('deductible_amount') }} as deductible_amount
        , cast(in_network_flag as integer) as in_network_flag
        , cast(data_source as {{ dbt.type_string() }}) as data_source
        , cast(file_name as {{ dbt.type_string() }}) as file_name
        , cast(ingest_datetime as {{ dbt.type_timestamp() }}) as ingest_datetime
    from deduped_claims

)

select
      claim_id
    , claim_line_number
    , person_id
    , member_id
    , payer
    , {{ the_tuva_project.quote_column('plan') }}
    , prescribing_provider_npi
    , dispensing_provider_npi
    , dispensing_date
    , ndc_code
    , quantity
    , days_supply
    , refills
    , paid_date
    , paid_amount
    , allowed_amount
    , charge_amount
    , coinsurance_amount
    , copayment_amount
    , deductible_amount
    , in_network_flag
    , data_source
    , file_name
    , ingest_datetime
from data_types