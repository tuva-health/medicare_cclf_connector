{#-
    Casts a column as string or varchar.
-#}

{%- macro cast_string_or_varchar(column_name) -%}

    {{ return(adapter.dispatch('cast_string_or_varchar')(column_name)) }}

{%- endmacro -%}

{%- macro bigquery__cast_string_or_varchar(column_name) -%}

    cast( {{ column_name }} as string )

{%- endmacro -%}

{%- macro default__cast_string_or_varchar(column_name) %}

    cast( {{ column_name }} as string )

{%- endmacro -%}

{%- macro redshift__cast_string_or_varchar(column_name) -%}

    cast( {{ column_name }} as varchar )

{%- endmacro -%}

{%- macro snowflake__cast_string_or_varchar(column_name) %}

    cast( {{ column_name }} as string )

{%- endmacro -%}