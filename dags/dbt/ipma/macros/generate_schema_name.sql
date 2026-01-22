{% macro generate_schema_name(custom_schema_name, node) -%}
  {# 
    Queremos schemas EXATOS: raw, curated, delivery (sem prefixo tipo curated_delivery).
    - Se o model define +schema: delivery -> usa "delivery"
    - Se não define -> usa target.schema (ex: "curated")
  #}

  {% if custom_schema_name is none %}
    {{ target.schema }}
  {% else %}
    {{ custom_schema_name | trim }}
  {% endif %}
{%- endmacro %}
