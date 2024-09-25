-- Usage: This macro is a WIP to periodically delete tables/views that exist in an environment, but not in our dbt repo 
--        (i.e. we no longer build a certain model, but it's still in the EDW)
-- Example for CLI use: 
--        dbt run-operation delete_outdated_tables --args '{schema: [edw, raw, staging]}'

{% macro delete_outdated_tables(schema) %} 
  {% if (schema is not string and schema is not iterable) or schema is mapping or schema|length <= 0 %}
    {% do exceptions.raise_compiler_error('"schema" must be a string or a list') %}
  {% endif %}
  {% if schema is string %}
    {% set schema = [schema] %}
  {% endif %}

  {% call statement('get_outdated_tables', fetch_result=True) %}
    select c.schema_name,
           c.ref_name,
           c.ref_type
    from (
        select table_schema as schema_name, 
           table_name as ref_name, 
             'table' as ref_type
      from information_schema.tables 
      where table_schema in (
        {%- for s in schema -%}
        UPPER('{{ s }}'){% if not loop.last %},{% endif %}
        {%- endfor -%}
      )
    union all
    select table_schema as schema_name, 
           table_name as ref_name, 
             'view' as ref_type
      from information_schema.views
        where table_schema in (
        {%- for s in schema -%}
        UPPER('{{ s }}'){% if not loop.last %},{% endif %}
        {%- endfor -%}
      )) as c
    left join (values
      {%- for node in graph['nodes'].values() | selectattr("resource_type", "equalto", "model") | list
                    + graph['nodes'].values() | selectattr("resource_type", "equalto", "seed")  | list %} 
        {% for s in schema %}
            (UPPER('{{ s }}'), UPPER('{{node.name}}')),
        {% endfor %}
        (UPPER('{{node.schema}}'), UPPER('{{node.name}}')){% if not loop.last %},{% endif %}
      {%- endfor %}
    ) as desired (schema_name, ref_name) on desired.schema_name = c.schema_name
                                        and desired.ref_name    = c.ref_name
    where desired.ref_name is null
  {% endcall %}

  {%- for to_delete in load_result('get_outdated_tables')['data'] %} 
    {% set fqn = target.database + '.' + to_delete[0] + '.' + to_delete[1] %}
--    {% if 'super secure table name' in fqn %}
--      {% do exceptions.raise_compiler_error('Was asked to drop a protected or super secure table we absolutely need to keep a version of, will not proceed. Table: ' + fqn) %}
--    {% endif %}
    {% call statement() -%}
      {% do log('dropping ' ~ to_delete[2] ~ ': ' ~ fqn, info=true) %}
      drop {{ to_delete[2] }} if exists {{ fqn }} cascade;
    {%- endcall %}
  {%- endfor %}

{% endmacro %}