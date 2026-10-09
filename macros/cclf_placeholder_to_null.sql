{#-
    CMS fills CCLF code and identifier fields that do not apply to a claim
    with the placeholder '~' (admission type and source on non-inpatient
    claims, unused diagnosis slots, operating/other NPI, modifier 5, POA
    indicators, ...). '~' is not a code, so the output models map it to null.

    Staging keeps '~' as delivered: the related-claims logic partitions and
    joins on raw source values (PRVDR_OSCAR_NUM, for one, joins the Part A
    files), and nulls would not match in those joins.
-#}

{%- macro cclf_placeholder_to_null(column_name) -%}

    case
        when trim(cast({{ column_name }} as {{ dbt.type_string() }})) = '~' then null
        else {{ column_name }}
    end

{%- endmacro -%}
