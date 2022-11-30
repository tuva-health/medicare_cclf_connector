{#-
    Casts a column as numeric with or without precision/scale depending on
    adapter type.
-#}

{%- macro cast_numeric(column_name) -%}

    {{ return(adapter.dispatch('cast_numeric')(column_name)) }}

{%- endmacro -%}

{%- macro bigquery__cast_numeric(column_name) -%}

    cast( {{ column_name }} as numeric )

{%- endmacro -%}

{%- macro default__cast_numeric(column_name) %}

    cast( {{ column_name }} as numeric(38,2) )

{%- endmacro -%}

{%- macro redshift__cast_numeric(column_name) -%}

    cast( {{ column_name }} as numeric(38,2) )

{%- endmacro -%}

{%- macro snowflake__cast_numeric(column_name) %}

    cast( {{ column_name }} as numeric(38,2) )

{%- endmacro -%}