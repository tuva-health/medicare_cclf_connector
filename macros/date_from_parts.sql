{#

    This macro provides a cross-database compatible way to create a date from parts. Uses datefromparts for most databases and
    make_date for DuckDB.

#}db

{%- macro date_from_parts(year, month, day=1) -%}

    {{ return(adapter.dispatch('date_from_parts')(year, month, day)) }}

{%- endmacro -%}

{%- macro duckdb__date_from_parts(year, month, day) -%}

    make_date( {{ year }}, {{ month }}, {{ day }} )

{%- endmacro -%}

{%- macro default__date_from_parts(year, month, day) -%}

    datefromparts( {{ year }}, {{ month }}, {{ day }} )

{%- endmacro -%}
