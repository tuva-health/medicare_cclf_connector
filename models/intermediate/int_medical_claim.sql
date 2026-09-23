with winning_keys as (

    select *
    from {{ ref('int_medical_claim_winning_keys') }}
    where row_num = 1

)

/*
    CCLFA (Part A benefit enhancement and demonstration codes) is claim level,
    but a claim appears on one row per related condition code and again in
    each monthly / run-out file. The amounts are identical on every row for a
    claim, so keep one row per claim, preferring the most recent file.
*/
, parta_demo_codes_ranked as (

    select
          cur_clm_uniq_id
        , clm_pbp_rdctn_amt
        , row_number() over (
            partition by cur_clm_uniq_id
            order by
                  file_date desc
                , file_name desc
          ) as row_num
    from {{ ref('stg_parta_demo_codes') }}
    where cur_clm_uniq_id is not null

)

, parta_demo_codes as (

    select
          cast(cur_clm_uniq_id as {{ dbt.type_string() }}) as claim_id
        , {{ cast_numeric('clm_pbp_rdctn_amt') }} as clm_pbp_rdctn_amt
    from parta_demo_codes_ranked
    where row_num = 1

)

/*
    CCLFB (Part B benefit enhancement and demonstration codes) is claim line
    level. clm_line_num is zero padded in the raw file ('01'), so dedupe and
    join on the integer value to match claim_line_number.
*/
, partb_demo_codes_ranked as (

    select
          cur_clm_uniq_id
        , {{ try_to_cast_int('clm_line_num') }} as claim_line_number
        , clm_pbp_rdctn_amt
        , row_number() over (
            partition by cur_clm_uniq_id, {{ try_to_cast_int('clm_line_num') }}
            order by
                  file_date desc
                , file_name desc
          ) as row_num
    from {{ ref('stg_partb_demo_codes') }}
    where cur_clm_uniq_id is not null

)

, partb_demo_codes as (

    select
          cast(cur_clm_uniq_id as {{ dbt.type_string() }}) as claim_id
        , claim_line_number
        , {{ cast_numeric('clm_pbp_rdctn_amt') }} as clm_pbp_rdctn_amt
    from partb_demo_codes_ranked
    where row_num = 1

)

, institutional_claims as (

    select
          institutional_claims.claim_id
        , institutional_claims.claim_line_number
        , institutional_claims.claim_type
        , institutional_claims.person_id
        , institutional_claims.member_id
        , institutional_claims.payer
        , institutional_claims.{{ quote_column('plan') }}
        , institutional_claims.claim_start_date
        , institutional_claims.claim_end_date
        , institutional_claims.claim_line_start_date
        , institutional_claims.claim_line_end_date
        , institutional_claims.admission_date
        , institutional_claims.discharge_date
        , institutional_claims.admit_source_code
        , institutional_claims.admit_type_code
        , institutional_claims.discharge_disposition_code
        , institutional_claims.place_of_service_code
        , institutional_claims.bill_type_code
        , institutional_claims.drg_code_type
        , institutional_claims.drg_code
        , institutional_claims.revenue_center_code
        , institutional_claims.service_unit_quantity
        , cast(null as {{ dbt.type_string() }}) as claim_provider_specialty_code
        , institutional_claims.hcpcs_code
        , institutional_claims.hcpcs_modifier_1
        , institutional_claims.hcpcs_modifier_2
        , institutional_claims.hcpcs_modifier_3
        , institutional_claims.hcpcs_modifier_4
        , institutional_claims.hcpcs_modifier_5
        , institutional_claims.ccn
        , institutional_claims.claim_type_code
        , institutional_claims.other_npi
        , institutional_claims.attending_npi
        , institutional_claims.operating_npi
        , institutional_claims.rendering_npi
        , institutional_claims.rendering_tin
        , institutional_claims.billing_npi
        , institutional_claims.billing_tin
        , institutional_claims.facility_npi
        , institutional_claims.paid_date
        , institutional_claims.paid_amount
        , institutional_claims.allowed_amount
        , institutional_claims.charge_amount
        , institutional_claims.coinsurance_amount
        , institutional_claims.copayment_amount
        , institutional_claims.deductible_amount
        , institutional_claims.total_cost_amount
        , case
            when institutional_claims.claim_line_number = 1 then parta_demo_codes.clm_pbp_rdctn_amt
            else null
          end as paid_reduced_by
        , institutional_claims.diagnosis_code_type
        , institutional_claims.diagnosis_code_1
        , institutional_claims.diagnosis_code_2
        , institutional_claims.diagnosis_code_3
        , institutional_claims.diagnosis_code_4
        , institutional_claims.diagnosis_code_5
        , institutional_claims.diagnosis_code_6
        , institutional_claims.diagnosis_code_7
        , institutional_claims.diagnosis_code_8
        , institutional_claims.diagnosis_code_9
        , institutional_claims.diagnosis_code_10
        , institutional_claims.diagnosis_code_11
        , institutional_claims.diagnosis_code_12
        , institutional_claims.diagnosis_code_13
        , institutional_claims.diagnosis_code_14
        , institutional_claims.diagnosis_code_15
        , institutional_claims.diagnosis_code_16
        , institutional_claims.diagnosis_code_17
        , institutional_claims.diagnosis_code_18
        , institutional_claims.diagnosis_code_19
        , institutional_claims.diagnosis_code_20
        , institutional_claims.diagnosis_code_21
        , institutional_claims.diagnosis_code_22
        , institutional_claims.diagnosis_code_23
        , institutional_claims.diagnosis_code_24
        , institutional_claims.diagnosis_code_25
        , institutional_claims.diagnosis_poa_1
        , institutional_claims.diagnosis_poa_2
        , institutional_claims.diagnosis_poa_3
        , institutional_claims.diagnosis_poa_4
        , institutional_claims.diagnosis_poa_5
        , institutional_claims.diagnosis_poa_6
        , institutional_claims.diagnosis_poa_7
        , institutional_claims.diagnosis_poa_8
        , institutional_claims.diagnosis_poa_9
        , institutional_claims.diagnosis_poa_10
        , institutional_claims.diagnosis_poa_11
        , institutional_claims.diagnosis_poa_12
        , institutional_claims.diagnosis_poa_13
        , institutional_claims.diagnosis_poa_14
        , institutional_claims.diagnosis_poa_15
        , institutional_claims.diagnosis_poa_16
        , institutional_claims.diagnosis_poa_17
        , institutional_claims.diagnosis_poa_18
        , institutional_claims.diagnosis_poa_19
        , institutional_claims.diagnosis_poa_20
        , institutional_claims.diagnosis_poa_21
        , institutional_claims.diagnosis_poa_22
        , institutional_claims.diagnosis_poa_23
        , institutional_claims.diagnosis_poa_24
        , institutional_claims.diagnosis_poa_25
        , institutional_claims.procedure_code_type
        , institutional_claims.procedure_code_1
        , institutional_claims.procedure_code_2
        , institutional_claims.procedure_code_3
        , institutional_claims.procedure_code_4
        , institutional_claims.procedure_code_5
        , institutional_claims.procedure_code_6
        , institutional_claims.procedure_code_7
        , institutional_claims.procedure_code_8
        , institutional_claims.procedure_code_9
        , institutional_claims.procedure_code_10
        , institutional_claims.procedure_code_11
        , institutional_claims.procedure_code_12
        , institutional_claims.procedure_code_13
        , institutional_claims.procedure_code_14
        , institutional_claims.procedure_code_15
        , institutional_claims.procedure_code_16
        , institutional_claims.procedure_code_17
        , institutional_claims.procedure_code_18
        , institutional_claims.procedure_code_19
        , institutional_claims.procedure_code_20
        , institutional_claims.procedure_code_21
        , institutional_claims.procedure_code_22
        , institutional_claims.procedure_code_23
        , institutional_claims.procedure_code_24
        , institutional_claims.procedure_code_25
        , institutional_claims.procedure_date_1
        , institutional_claims.procedure_date_2
        , institutional_claims.procedure_date_3
        , institutional_claims.procedure_date_4
        , institutional_claims.procedure_date_5
        , institutional_claims.procedure_date_6
        , institutional_claims.procedure_date_7
        , institutional_claims.procedure_date_8
        , institutional_claims.procedure_date_9
        , institutional_claims.procedure_date_10
        , institutional_claims.procedure_date_11
        , institutional_claims.procedure_date_12
        , institutional_claims.procedure_date_13
        , institutional_claims.procedure_date_14
        , institutional_claims.procedure_date_15
        , institutional_claims.procedure_date_16
        , institutional_claims.procedure_date_17
        , institutional_claims.procedure_date_18
        , institutional_claims.procedure_date_19
        , institutional_claims.procedure_date_20
        , institutional_claims.procedure_date_21
        , institutional_claims.procedure_date_22
        , institutional_claims.procedure_date_23
        , institutional_claims.procedure_date_24
        , institutional_claims.procedure_date_25
        , institutional_claims.in_network_flag
        , institutional_claims.data_source
        , institutional_claims.file_name
        , institutional_claims.file_date
        , institutional_claims.ingest_datetime
        , winning_keys.row_num
    from {{ ref('int_institutional_claim_deduped') }} institutional_claims
    inner join winning_keys
        on institutional_claims.branch_row_key = winning_keys.branch_row_key
    left join parta_demo_codes
        on institutional_claims.claim_id = parta_demo_codes.claim_id

)

, physician_claims as (

    select
          physician_claims.claim_id
        , physician_claims.claim_line_number
        , physician_claims.claim_type
        , physician_claims.person_id
        , physician_claims.member_id
        , physician_claims.payer
        , physician_claims.{{ quote_column('plan') }}
        , physician_claims.claim_start_date
        , physician_claims.claim_end_date
        , physician_claims.claim_line_start_date
        , physician_claims.claim_line_end_date
        , physician_claims.admission_date
        , physician_claims.discharge_date
        , physician_claims.admit_source_code
        , physician_claims.admit_type_code
        , physician_claims.discharge_disposition_code
        , physician_claims.place_of_service_code
        , physician_claims.bill_type_code
        , cast(null as {{ dbt.type_string() }}) as drg_code_type
        , cast(null as {{ dbt.type_string() }}) as drg_code
        , physician_claims.revenue_center_code
        , physician_claims.service_unit_quantity
        , physician_claims.claim_provider_specialty_code
        , physician_claims.hcpcs_code
        , physician_claims.hcpcs_modifier_1
        , physician_claims.hcpcs_modifier_2
        , physician_claims.hcpcs_modifier_3
        , physician_claims.hcpcs_modifier_4
        , physician_claims.hcpcs_modifier_5
        , cast(null as {{ dbt.type_string() }}) as ccn
        , physician_claims.clm_type_cd as claim_type_code
        , cast(null as {{ dbt.type_string() }}) as other_npi
        , cast(null as {{ dbt.type_string() }}) as attending_npi
        , cast(null as {{ dbt.type_string() }}) as operating_npi
        , physician_claims.rendering_npi
        , physician_claims.rendering_tin
        , physician_claims.billing_npi
        , physician_claims.billing_tin
        , physician_claims.facility_npi
        , physician_claims.paid_date
        , physician_claims.paid_amount
        , physician_claims.allowed_amount
        , physician_claims.charge_amount
        , physician_claims.coinsurance_amount
        , physician_claims.copayment_amount
        , physician_claims.deductible_amount
        , physician_claims.total_cost_amount
        , partb_demo_codes.clm_pbp_rdctn_amt as paid_reduced_by
        , physician_claims.diagnosis_code_type
        , physician_claims.diagnosis_code_1
        , physician_claims.diagnosis_code_2
        , physician_claims.diagnosis_code_3
        , physician_claims.diagnosis_code_4
        , physician_claims.diagnosis_code_5
        , physician_claims.diagnosis_code_6
        , physician_claims.diagnosis_code_7
        , physician_claims.diagnosis_code_8
        , physician_claims.diagnosis_code_9
        , physician_claims.diagnosis_code_10
        , physician_claims.diagnosis_code_11
        , physician_claims.diagnosis_code_12
        , physician_claims.diagnosis_code_13
        , physician_claims.diagnosis_code_14
        , physician_claims.diagnosis_code_15
        , physician_claims.diagnosis_code_16
        , physician_claims.diagnosis_code_17
        , physician_claims.diagnosis_code_18
        , physician_claims.diagnosis_code_19
        , physician_claims.diagnosis_code_20
        , physician_claims.diagnosis_code_21
        , physician_claims.diagnosis_code_22
        , physician_claims.diagnosis_code_23
        , physician_claims.diagnosis_code_24
        , physician_claims.diagnosis_code_25
        , physician_claims.diagnosis_poa_1
        , physician_claims.diagnosis_poa_2
        , physician_claims.diagnosis_poa_3
        , physician_claims.diagnosis_poa_4
        , physician_claims.diagnosis_poa_5
        , physician_claims.diagnosis_poa_6
        , physician_claims.diagnosis_poa_7
        , physician_claims.diagnosis_poa_8
        , physician_claims.diagnosis_poa_9
        , physician_claims.diagnosis_poa_10
        , physician_claims.diagnosis_poa_11
        , physician_claims.diagnosis_poa_12
        , physician_claims.diagnosis_poa_13
        , physician_claims.diagnosis_poa_14
        , physician_claims.diagnosis_poa_15
        , physician_claims.diagnosis_poa_16
        , physician_claims.diagnosis_poa_17
        , physician_claims.diagnosis_poa_18
        , physician_claims.diagnosis_poa_19
        , physician_claims.diagnosis_poa_20
        , physician_claims.diagnosis_poa_21
        , physician_claims.diagnosis_poa_22
        , physician_claims.diagnosis_poa_23
        , physician_claims.diagnosis_poa_24
        , physician_claims.diagnosis_poa_25
        , physician_claims.procedure_code_type
        , physician_claims.procedure_code_1
        , physician_claims.procedure_code_2
        , physician_claims.procedure_code_3
        , physician_claims.procedure_code_4
        , physician_claims.procedure_code_5
        , physician_claims.procedure_code_6
        , physician_claims.procedure_code_7
        , physician_claims.procedure_code_8
        , physician_claims.procedure_code_9
        , physician_claims.procedure_code_10
        , physician_claims.procedure_code_11
        , physician_claims.procedure_code_12
        , physician_claims.procedure_code_13
        , physician_claims.procedure_code_14
        , physician_claims.procedure_code_15
        , physician_claims.procedure_code_16
        , physician_claims.procedure_code_17
        , physician_claims.procedure_code_18
        , physician_claims.procedure_code_19
        , physician_claims.procedure_code_20
        , physician_claims.procedure_code_21
        , physician_claims.procedure_code_22
        , physician_claims.procedure_code_23
        , physician_claims.procedure_code_24
        , physician_claims.procedure_code_25
        , physician_claims.procedure_date_1
        , physician_claims.procedure_date_2
        , physician_claims.procedure_date_3
        , physician_claims.procedure_date_4
        , physician_claims.procedure_date_5
        , physician_claims.procedure_date_6
        , physician_claims.procedure_date_7
        , physician_claims.procedure_date_8
        , physician_claims.procedure_date_9
        , physician_claims.procedure_date_10
        , physician_claims.procedure_date_11
        , physician_claims.procedure_date_12
        , physician_claims.procedure_date_13
        , physician_claims.procedure_date_14
        , physician_claims.procedure_date_15
        , physician_claims.procedure_date_16
        , physician_claims.procedure_date_17
        , physician_claims.procedure_date_18
        , physician_claims.procedure_date_19
        , physician_claims.procedure_date_20
        , physician_claims.procedure_date_21
        , physician_claims.procedure_date_22
        , physician_claims.procedure_date_23
        , physician_claims.procedure_date_24
        , physician_claims.procedure_date_25
        , physician_claims.in_network_flag
        , physician_claims.data_source
        , physician_claims.file_name
        , {{ try_to_cast_date('physician_claims.ingest_datetime') }} as file_date
        , physician_claims.ingest_datetime
        , winning_keys.row_num
    from {{ ref('int_physician_claim_deduped') }} physician_claims
    inner join winning_keys
        on physician_claims.branch_row_key = winning_keys.branch_row_key
    left join partb_demo_codes
        on physician_claims.claim_id = partb_demo_codes.claim_id
        and physician_claims.claim_line_number = partb_demo_codes.claim_line_number

)

, dme_claims as (

    select
          dme_claims.claim_id
        , dme_claims.claim_line_number
        , dme_claims.claim_type
        , dme_claims.person_id
        , dme_claims.member_id
        , dme_claims.payer
        , dme_claims.{{ quote_column('plan') }}
        , dme_claims.claim_start_date
        , dme_claims.claim_end_date
        , dme_claims.claim_line_start_date
        , dme_claims.claim_line_end_date
        , dme_claims.admission_date
        , dme_claims.discharge_date
        , dme_claims.admit_source_code
        , dme_claims.admit_type_code
        , dme_claims.discharge_disposition_code
        , dme_claims.place_of_service_code
        , dme_claims.bill_type_code
        , cast(null as {{ dbt.type_string() }}) as drg_code_type
        , cast(null as {{ dbt.type_string() }}) as drg_code
        , dme_claims.revenue_center_code
        , dme_claims.service_unit_quantity
        , cast(null as {{ dbt.type_string() }}) as claim_provider_specialty_code
        , dme_claims.hcpcs_code
        , dme_claims.hcpcs_modifier_1
        , dme_claims.hcpcs_modifier_2
        , dme_claims.hcpcs_modifier_3
        , dme_claims.hcpcs_modifier_4
        , dme_claims.hcpcs_modifier_5
        , cast(null as {{ dbt.type_string() }}) as ccn
        , cast(null as {{ dbt.type_string() }}) as claim_type_code
        , cast(null as {{ dbt.type_string() }}) as other_npi
        , cast(null as {{ dbt.type_string() }}) as attending_npi
        , cast(null as {{ dbt.type_string() }}) as operating_npi
        , dme_claims.rendering_npi
        , dme_claims.rendering_tin
        , dme_claims.billing_npi
        , dme_claims.billing_tin
        , dme_claims.facility_npi
        , dme_claims.paid_date
        , dme_claims.paid_amount
        , dme_claims.allowed_amount
        , dme_claims.charge_amount
        , dme_claims.coinsurance_amount
        , dme_claims.copayment_amount
        , dme_claims.deductible_amount
        , dme_claims.total_cost_amount
        , partb_demo_codes.clm_pbp_rdctn_amt as paid_reduced_by
        , dme_claims.diagnosis_code_type
        , dme_claims.diagnosis_code_1
        , dme_claims.diagnosis_code_2
        , dme_claims.diagnosis_code_3
        , dme_claims.diagnosis_code_4
        , dme_claims.diagnosis_code_5
        , dme_claims.diagnosis_code_6
        , dme_claims.diagnosis_code_7
        , dme_claims.diagnosis_code_8
        , dme_claims.diagnosis_code_9
        , dme_claims.diagnosis_code_10
        , dme_claims.diagnosis_code_11
        , dme_claims.diagnosis_code_12
        , dme_claims.diagnosis_code_13
        , dme_claims.diagnosis_code_14
        , dme_claims.diagnosis_code_15
        , dme_claims.diagnosis_code_16
        , dme_claims.diagnosis_code_17
        , dme_claims.diagnosis_code_18
        , dme_claims.diagnosis_code_19
        , dme_claims.diagnosis_code_20
        , dme_claims.diagnosis_code_21
        , dme_claims.diagnosis_code_22
        , dme_claims.diagnosis_code_23
        , dme_claims.diagnosis_code_24
        , dme_claims.diagnosis_code_25
        , dme_claims.diagnosis_poa_1
        , dme_claims.diagnosis_poa_2
        , dme_claims.diagnosis_poa_3
        , dme_claims.diagnosis_poa_4
        , dme_claims.diagnosis_poa_5
        , dme_claims.diagnosis_poa_6
        , dme_claims.diagnosis_poa_7
        , dme_claims.diagnosis_poa_8
        , dme_claims.diagnosis_poa_9
        , dme_claims.diagnosis_poa_10
        , dme_claims.diagnosis_poa_11
        , dme_claims.diagnosis_poa_12
        , dme_claims.diagnosis_poa_13
        , dme_claims.diagnosis_poa_14
        , dme_claims.diagnosis_poa_15
        , dme_claims.diagnosis_poa_16
        , dme_claims.diagnosis_poa_17
        , dme_claims.diagnosis_poa_18
        , dme_claims.diagnosis_poa_19
        , dme_claims.diagnosis_poa_20
        , dme_claims.diagnosis_poa_21
        , dme_claims.diagnosis_poa_22
        , dme_claims.diagnosis_poa_23
        , dme_claims.diagnosis_poa_24
        , dme_claims.diagnosis_poa_25
        , dme_claims.procedure_code_type
        , dme_claims.procedure_code_1
        , dme_claims.procedure_code_2
        , dme_claims.procedure_code_3
        , dme_claims.procedure_code_4
        , dme_claims.procedure_code_5
        , dme_claims.procedure_code_6
        , dme_claims.procedure_code_7
        , dme_claims.procedure_code_8
        , dme_claims.procedure_code_9
        , dme_claims.procedure_code_10
        , dme_claims.procedure_code_11
        , dme_claims.procedure_code_12
        , dme_claims.procedure_code_13
        , dme_claims.procedure_code_14
        , dme_claims.procedure_code_15
        , dme_claims.procedure_code_16
        , dme_claims.procedure_code_17
        , dme_claims.procedure_code_18
        , dme_claims.procedure_code_19
        , dme_claims.procedure_code_20
        , dme_claims.procedure_code_21
        , dme_claims.procedure_code_22
        , dme_claims.procedure_code_23
        , dme_claims.procedure_code_24
        , dme_claims.procedure_code_25
        , dme_claims.procedure_date_1
        , dme_claims.procedure_date_2
        , dme_claims.procedure_date_3
        , dme_claims.procedure_date_4
        , dme_claims.procedure_date_5
        , dme_claims.procedure_date_6
        , dme_claims.procedure_date_7
        , dme_claims.procedure_date_8
        , dme_claims.procedure_date_9
        , dme_claims.procedure_date_10
        , dme_claims.procedure_date_11
        , dme_claims.procedure_date_12
        , dme_claims.procedure_date_13
        , dme_claims.procedure_date_14
        , dme_claims.procedure_date_15
        , dme_claims.procedure_date_16
        , dme_claims.procedure_date_17
        , dme_claims.procedure_date_18
        , dme_claims.procedure_date_19
        , dme_claims.procedure_date_20
        , dme_claims.procedure_date_21
        , dme_claims.procedure_date_22
        , dme_claims.procedure_date_23
        , dme_claims.procedure_date_24
        , dme_claims.procedure_date_25
        , dme_claims.in_network_flag
        , dme_claims.data_source
        , dme_claims.file_name
        , {{ try_to_cast_date('dme_claims.ingest_datetime') }} as file_date
        , dme_claims.ingest_datetime
        , winning_keys.row_num
    from {{ ref('int_dme_claim_deduped') }} dme_claims
    inner join winning_keys
        on dme_claims.branch_row_key = winning_keys.branch_row_key
    left join partb_demo_codes
        on dme_claims.claim_id = partb_demo_codes.claim_id
        and dme_claims.claim_line_number = partb_demo_codes.claim_line_number

)

select * from institutional_claims
union all
select * from physician_claims
union all
select * from dme_claims
