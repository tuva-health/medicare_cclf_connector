{#
    Creates the schemas dbt unit tests need before anything has been built.

    The unit materialization creates a temporary table next to the model under
    test (in the model's own database and schema), and a model without a custom
    schema lands in target.schema. Every connector model has a custom,
    per-run-prefixed schema, so on a fresh CI database none of these exist when
    `dbt test --select test_type:unit` runs first. `dbt run` and `dbt build`
    create the schemas of selected models, but `dbt test` creates none, and
    warehouses whose temp tables ignore the schema (DuckDB) hide the gap.

    Runs as an on-run-start hook. Creating a schema that exists is a no-op, and
    every schema created here carries the run's prefix, so drop_ci_schemas
    removes it.
#}
{% macro ensure_unit_test_schemas() -%}
    {%- if execute -%}
        {%- set relations = [api.Relation.create(database=target.database, schema=target.schema)] -%}
        {%- set seen = [(target.database ~ '.' ~ target.schema) | lower] -%}

        {#- unit_test.<package>.<model>.<test>; graph has no unit_tests key. -#}
        {%- for unique_id in selected_resources if unique_id.startswith('unit_test.') -%}
            {%- set parts = unique_id.split('.') -%}
            {%- set model = graph.nodes.get('model.' ~ parts[1] ~ '.' ~ parts[2]) -%}
            {%- if model is none -%}
                {{ exceptions.raise_compiler_error("ensure_unit_test_schemas: no model found for " ~ unique_id) }}
            {%- endif -%}
            {%- set key = (model.database ~ '.' ~ model.schema) | lower -%}
            {%- if key not in seen -%}
                {%- do seen.append(key) -%}
                {%- do relations.append(api.Relation.create(database=model.database, schema=model.schema)) -%}
            {%- endif -%}
        {%- endfor -%}

        {%- for relation in relations -%}
            {%- do adapter.create_schema(relation) -%}
        {%- endfor -%}
    {%- endif -%}
{%- endmacro %}
