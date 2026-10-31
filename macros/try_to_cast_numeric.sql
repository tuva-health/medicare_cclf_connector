{#-
    Safely casts source values to numeric or integer types after normalizing
    blanks to null.
-#}

{%- macro try_to_cast_numeric(column_name) -%}

    {{ dbt.safe_cast(clean_source_value(column_name), dbt.type_numeric()) }}

{%- endmacro -%}

{%- macro try_to_cast_int(column_name) -%}

    {{ dbt.safe_cast(clean_source_value(column_name), dbt.type_int()) }}

{%- endmacro -%}

{#-
    A fraction that needs more than the two decimals of cast_numeric, such
    as ALR dual person-years (months / 12). numeric(38,10) keeps the ten
    decimals CMS delivers; BigQuery's NUMERIC allows only nine.
-#}

{%- macro try_to_cast_fraction(column_name) -%}

    {{ dbt.safe_cast(clean_source_value(column_name), fraction_type()) }}

{%- endmacro -%}

{%- macro fraction_type() -%}

    {{ return(adapter.dispatch('fraction_type')()) }}

{%- endmacro -%}

{%- macro default__fraction_type() -%}

    numeric(38,10)

{%- endmacro -%}

{%- macro bigquery__fraction_type() -%}

    bignumeric

{%- endmacro -%}
