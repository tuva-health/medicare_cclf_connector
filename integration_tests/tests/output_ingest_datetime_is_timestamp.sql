/*
    The Tuva Input Layer types ingest_datetime as a timestamp on
    medical_claim, pharmacy_claim and eligibility (input_layer__*.yml,
    data_type: timestamp), and the_tuva_project's
    data_quality__structural_data_type_mismatches model reports any output
    column whose warehouse type differs from that.

    Returns one row per output whose ingest_datetime column is missing or is
    not a timestamp.
*/

{%- set outputs = [] -%}
{%- for output in ['medical_claim', 'pharmacy_claim', 'eligibility'] -%}
    {%- do outputs.append((output, ref(output))) -%}
{%- endfor -%}

{%- set mismatches = [] -%}
{%- if execute -%}
    {%- for output, relation in outputs -%}
        {%- set data_type = [] -%}
        {%- for column in adapter.get_columns_in_relation(relation) -%}
            {%- if column.name | lower == 'ingest_datetime' -%}
                {%- do data_type.append(column.data_type | lower) -%}
            {%- endif -%}
        {%- endfor -%}
        {%- if data_type | length == 0 -%}
            {%- do mismatches.append((output, 'missing')) -%}
        {%- elif not data_type[0].startswith('timestamp') -%}
            {%- do mismatches.append((output, data_type[0])) -%}
        {%- endif -%}
    {%- endfor -%}
{%- endif %}

{% if mismatches | length > 0 -%}
{%- for output, data_type in mismatches %}
{% if not loop.first %}union all
{% endif -%}
select
      '{{ output }}' as output_model
    , '{{ data_type }}' as ingest_datetime_data_type
{%- endfor %}
{%- else -%}
select
      cast(null as {{ dbt.type_string() }}) as output_model
    , cast(null as {{ dbt.type_string() }}) as ingest_datetime_data_type
where 1 = 0
{%- endif %}
