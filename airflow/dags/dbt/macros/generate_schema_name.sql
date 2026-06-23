{% macro generate_schema_name(custom_schema_name, node) %}
  {#
    Override dbt's default schema naming so model-level schema names are used as-is
    and stage models inherit the target schema directly instead of becoming
    `<target.schema>_<model_path>`.
  #}
  {% if custom_schema_name %}
    {{ return(custom_schema_name) }}
  {% else %}
    {{ return(target.schema) }}
  {% endif %}
{% endmacro %}
