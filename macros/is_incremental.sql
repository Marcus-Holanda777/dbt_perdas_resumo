{% macro is_incremental(
    schema="prevencao-perdas", name="resumo_perdas"
) %}

    {% set stmt %}
        select 1 from information_schema.tables
        where table_schema = '{{ schema }}'
        and table_name = '{{ name }}'
    {% endset %}

    {% if execute %}
        {% set if_exists = run_query(stmt) %}
        {% if if_exists | length > 0 %} {{ return(true) }}
        {% else %} {{ return(false) }}
        {% endif %}
    {% endif %}

{% endmacro %}
