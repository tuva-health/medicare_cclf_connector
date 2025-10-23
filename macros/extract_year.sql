{% macro extract_year(date_col) %}
    {% if target.type in ['postgres', 'redshift'] %}
        EXTRACT(YEAR FROM {{ date_col }})
    {% elif target.type in ['snowflake', 'fabric'] %}
        YEAR({{ date_col }})
    {% elif target.type == 'bigquery' %}
        EXTRACT(YEAR FROM {{ date_col }})
    {% elif target.type in ['databricks', 'spark'] %}
        YEAR({{ date_col }})
    {% elif target.type == 'duckdb' %}
        EXTRACT(YEAR FROM {{ date_col }})
    {% else %}
        EXTRACT(YEAR FROM {{ date_col }})
    {% endif %}
{% endmacro %}