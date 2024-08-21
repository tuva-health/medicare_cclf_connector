select
      cast(null as {{ dbt.type_string() }}) as claim_id
    , cast(null as {{ dbt.type_string() }}) as claim_line_number
    , cast(null as {{ dbt.type_string() }}) as patient_id
    , cast(null as {{ dbt.type_string() }}) as member_id
    , cast(null as {{ dbt.type_string() }}) as payer
    , cast(null as {{ dbt.type_string() }}) as plan
    , cast(null as {{ dbt.type_string() }}) as prescribing_provider_npi
    , cast(null as {{ dbt.type_string() }}) as dispensing_provider_npi
    , cast(null as date ) as dispensing_date
    , cast(null as {{ dbt.type_string() }}) as ndc_code
    , cast(null as int) as quantity
    , cast(null as int) as days_supply
    , cast(null as int) as refills
    , cast(null as date) as paid_date
    , cast(null as numeric) as paid_amount
    , cast(null as numeric) as allowed_amount
    , cast(null as numeric) as charge_amount
    , cast(null as numeric) as coinsurance_amount
    , cast(null as numeric) as copayment_amount
    , cast(null as numeric) as deductible_amount
    , cast(1 as int) as in_network_flag
    , cast(null as {{ dbt.type_string() }}) as data_source
    , cast(null as {{ dbt.type_string() }}) as file_name
    , cast(null as {{ dbt.type_string() }}) as ingest_datetime
limit 0