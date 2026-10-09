select
      claim_id
    , claim_line_number
    , claim_type
    , person_id
    , member_id
    , payer
    , {{ quote_column('plan') }}
    , claim_start_date
    , claim_end_date
    , claim_line_start_date
    , claim_line_end_date
    , admission_date
    , discharge_date
    , {{ cclf_placeholder_to_null('admit_source_code') }} as admit_source_code
    , {{ cclf_placeholder_to_null('admit_type_code') }} as admit_type_code
    , {{ cclf_placeholder_to_null('discharge_disposition_code') }} as discharge_disposition_code
    , {{ cclf_placeholder_to_null('place_of_service_code') }} as place_of_service_code
    , {{ cclf_placeholder_to_null('bill_type_code') }} as bill_type_code
    , drg_code_type
    , {{ cclf_placeholder_to_null('drg_code') }} as drg_code
    , {{ cclf_placeholder_to_null('revenue_center_code') }} as revenue_center_code
    , service_unit_quantity
    , {{ cclf_placeholder_to_null('claim_provider_specialty_code') }} as claim_provider_specialty_code
    , {{ cclf_placeholder_to_null('hcpcs_code') }} as hcpcs_code
    , {{ cclf_placeholder_to_null('hcpcs_modifier_1') }} as hcpcs_modifier_1
    , {{ cclf_placeholder_to_null('hcpcs_modifier_2') }} as hcpcs_modifier_2
    , {{ cclf_placeholder_to_null('hcpcs_modifier_3') }} as hcpcs_modifier_3
    , {{ cclf_placeholder_to_null('hcpcs_modifier_4') }} as hcpcs_modifier_4
    , {{ cclf_placeholder_to_null('hcpcs_modifier_5') }} as hcpcs_modifier_5
    , case
        when {{ cclf_placeholder_to_null('ccn') }} is not null
        then right(concat('000000', ccn), 6)
      end as ccn
    , claim_type_code
    , {{ cclf_placeholder_to_null('other_npi') }} as other_npi
    , {{ cclf_placeholder_to_null('attending_npi') }} as attending_npi
    , {{ cclf_placeholder_to_null('operating_npi') }} as operating_npi
    , {{ cclf_placeholder_to_null('rendering_npi') }} as rendering_npi
    , {{ cclf_placeholder_to_null('rendering_tin') }} as rendering_tin
    , {{ cclf_placeholder_to_null('billing_npi') }} as billing_npi
    , {{ cclf_placeholder_to_null('billing_tin') }} as billing_tin
    , {{ cclf_placeholder_to_null('facility_npi') }} as facility_npi
    , paid_date
    , paid_amount
    , allowed_amount
    , charge_amount
    , coinsurance_amount
    , copayment_amount
    , deductible_amount
    , total_cost_amount
    , paid_reduced_by
    , diagnosis_code_type
    {%- for i in range(1, 26) %}
    , {{ cclf_placeholder_to_null('diagnosis_code_' ~ i) }} as diagnosis_code_{{ i }}
    {%- endfor %}
    {%- for i in range(1, 26) %}
    , {{ cclf_placeholder_to_null('diagnosis_poa_' ~ i) }} as diagnosis_poa_{{ i }}
    {%- endfor %}
    , procedure_code_type
    {%- for i in range(1, 26) %}
    , {{ cclf_placeholder_to_null('procedure_code_' ~ i) }} as procedure_code_{{ i }}
    {%- endfor %}
    , procedure_date_1
    , procedure_date_2
    , procedure_date_3
    , procedure_date_4
    , procedure_date_5
    , procedure_date_6
    , procedure_date_7
    , procedure_date_8
    , procedure_date_9
    , procedure_date_10
    , procedure_date_11
    , procedure_date_12
    , procedure_date_13
    , procedure_date_14
    , procedure_date_15
    , procedure_date_16
    , procedure_date_17
    , procedure_date_18
    , procedure_date_19
    , procedure_date_20
    , procedure_date_21
    , procedure_date_22
    , procedure_date_23
    , procedure_date_24
    , procedure_date_25
    , in_network_flag
    , 'medicare' as data_source
    , file_name
    , cast(file_date as date) as file_date
    , ingest_datetime
    , data_source as x_file_type
from {{ ref('int_medical_claim') }}
where row_num = 1