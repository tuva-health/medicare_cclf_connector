{{ config(tags=['fixture', 'tuva-95']) }}

/*
    TUVA-95: CMS fills dates that are not required or not available with
    1000-01-01 or 9999-12-31, and these "should be treated as missing or null
    values" (CCLF IP v43 3.6). The fixtures carry 1000-01-01 on inpatient
    revenue-center line dates, CCLFA active-care dates and most Part D
    effective dates, and 9999-12-31 as an XREF obsolete date.

    Neither sentinel may reach a date column of the output models.
*/

{%- set date_columns = {
    'medical_claim': [
        'claim_start_date',
        'claim_end_date',
        'claim_line_start_date',
        'claim_line_end_date',
        'admission_date',
        'discharge_date',
        'paid_date',
        'file_date',
    ],
    'pharmacy_claim': [
        'dispensing_date',
        'paid_date',
        'file_date',
    ],
    'eligibility': [
        'birth_date',
        'death_date',
        'enrollment_start_date',
        'enrollment_end_date',
        'file_date',
    ],
} -%}
{%- for i in range(1, 26) -%}
    {%- do date_columns['medical_claim'].append('procedure_date_' ~ i) -%}
{%- endfor %}

{% for model_name, columns in date_columns.items() -%}
{% for column in columns -%}
select
      '{{ model_name }}' as model_name
    , '{{ column }}' as column_name
    , count(*) as sentinel_rows
from {{ ref(model_name) }}
where cast({{ column }} as date) in (cast('1000-01-01' as date), cast('9999-12-31' as date))
having count(*) > 0
{% if not loop.last %}union all
{% endif %}
{%- endfor %}
{% if not loop.last %}union all
{% endif %}
{%- endfor %}
