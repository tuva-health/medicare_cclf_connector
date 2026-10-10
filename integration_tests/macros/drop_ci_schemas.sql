{#
    Drops every schema a CI run created in target.database. With
    tuva_schema_prefix set to <prefix>, the connector and the_tuva_project
    write to <prefix>_* (e.g. <prefix>_raw, <prefix>_core) and _<prefix>_*
    (e.g. _<prefix>_stg_input_layer). Only prefixes starting with ci_ are
    accepted so a mistyped call cannot drop shared schemas.

    dbt run-operation drop_ci_schemas --args '{prefix: ci_pr_1_abcdef12_r1_a1}'
#}
{% macro drop_ci_schemas(prefix) -%}
    {%- set prefix = (prefix or '') | trim | lower -%}
    {%- if not modules.re.fullmatch('ci_[a-z0-9_]+', prefix) -%}
        {{ exceptions.raise_compiler_error("drop_ci_schemas: refusing prefix '" ~ prefix ~ "'; it must match ci_[a-z0-9_]+") }}
    {%- endif -%}

    {%- set query -%}
        select schema_name
        from information_schema.schemata
        where lower(catalog_name) = lower('{{ target.database }}')
    {%- endset -%}

    {#- Filter here, not with SQL LIKE: _ is a LIKE wildcard and escape syntax differs by warehouse. -#}
    {%- set schemas = [] -%}
    {%- for schema in run_query(query).columns[0].values() -%}
        {%- set name = schema | lower -%}
        {%- if name.startswith(prefix ~ '_') or name.startswith('_' ~ prefix ~ '_') -%}
            {%- do schemas.append(schema) -%}
        {%- endif -%}
    {%- endfor -%}

    {%- for schema in schemas -%}
        {%- do log("Dropping schema " ~ target.database ~ "." ~ schema, info=true) -%}
        {%- do adapter.drop_schema(api.Relation.create(database=target.database, schema=schema)) -%}
    {%- endfor -%}
    {%- do log("Dropped " ~ (schemas | length) ~ " schema(s) for prefix " ~ prefix, info=true) -%}
{%- endmacro %}
