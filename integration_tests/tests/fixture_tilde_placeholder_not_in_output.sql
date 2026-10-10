{{ config(tags=['fixture', 'tuva-95']) }}

/*
    TUVA-95: CMS fills code and identifier fields that do not apply with the
    placeholder '~'. The fixtures use it where the CCLF files do: admission
    type and source on non-inpatient claims, operating/other NPI,
    HCPCS_5_MDFR_CD, unused CLM_DGNS_n_CD slots, present-on-admission
    indicators and Part D dispensing status.

    '~' is not a code. It must not reach the output models, and a claim whose
    admission type is '~' has no admission, so it must not get an admission
    or discharge date.
*/

{%- set medical_columns = [
    'admit_source_code',
    'admit_type_code',
    'discharge_disposition_code',
    'place_of_service_code',
    'bill_type_code',
    'drg_code',
    'revenue_center_code',
    'claim_provider_specialty_code',
    'hcpcs_code',
    'ccn',
    'other_npi',
    'attending_npi',
    'operating_npi',
    'rendering_npi',
    'rendering_tin',
    'billing_npi',
    'billing_tin',
    'facility_npi',
] -%}
{%- for i in range(1, 6) -%}
    {%- do medical_columns.append('hcpcs_modifier_' ~ i) -%}
{%- endfor -%}
{%- for i in range(1, 26) -%}
    {%- do medical_columns.append('diagnosis_code_' ~ i) -%}
    {%- do medical_columns.append('diagnosis_poa_' ~ i) -%}
    {%- do medical_columns.append('procedure_code_' ~ i) -%}
{%- endfor -%}
{%- set pharmacy_columns = [
    'prescribing_provider_npi',
    'dispensing_provider_npi',
    'ndc_code',
] %}

with placeholder_values as (

    {% for column in medical_columns -%}
    select
          'medical_claim' as model_name
        , claim_id
        , claim_line_number
        , '{{ column }}' as column_name
    from {{ ref('medical_claim') }}
    where {{ column }} like '%~%'
    union all
    {% endfor -%}
    {% for column in pharmacy_columns -%}
    select
          'pharmacy_claim' as model_name
        , claim_id
        , claim_line_number
        , '{{ column }}' as column_name
    from {{ ref('pharmacy_claim') }}
    where {{ column }} like '%~%'
    {% if not loop.last %}union all{% endif %}
    {% endfor %}

)

, placeholder_admissions as (

    select
          'medical_claim' as model_name
        , medical_claim.claim_id
        , medical_claim.claim_line_number
        , 'admission_date / discharge_date' as column_name
    from {{ ref('medical_claim') }} as medical_claim
    inner join {{ ref('parta_claims_header') }} as parta_claims_header
        on medical_claim.claim_id = parta_claims_header.cur_clm_uniq_id
    where trim(parta_claims_header.clm_admsn_type_cd) = '~'
      and (
          medical_claim.admission_date is not null
          or medical_claim.discharge_date is not null
      )

)

select distinct * from placeholder_values
union all
select distinct * from placeholder_admissions
