{%- macro snapshot_to_scd3(snapshot_ref, entity_key, valid_from_col='dbt_valid_from', track_columns=[]) -%}

{# This macro standardizes a dbt snapshot into a Type III SCD table: one row per entity,
   with current value and effective_from per tracked column. For each tracked column,
   effective_from is the earliest dbt_valid_from at which that column had that value
   for that entity (i.e. when the current value first appeared in history).

   snapshot_ref: ref to the snapshot table (e.g. ref('orders_snapshot')).
   entity_key: column(s) that uniquely identify the entity (single string or list).
   valid_from_col: snapshot column for valid-from timestamp (default dbt_valid_from).
   track_columns: list of column names to expose as current + effective_from (e.g. ['status','amount']).
   #}
{%- set entity_partition = [entity_key] if entity_key is string else entity_key -%}
{%- set partition_expr = entity_partition | join(', ') -%}

with

current as (
    select *
    from {{ snapshot_ref }}
    qualify row_number() over(
        partition by {{ partition_expr }}
        order by {{ valid_from_col }} desc
    ) = 1
)

{% for col in track_columns %},
{{ col }}_effective as (
    select
        {{ partition_expr }},
        {{ col }} as _val,
        min({{ valid_from_col }}) as {{ col }}_effective_from
    from {{ snapshot_ref }}
    group by {{ partition_expr }}, {{ col }}
)
{% endfor %}

,final as (
    select
        {% for k in entity_partition %}
        current.{{ k }},
        {% endfor %}
        current.{{ valid_from_col }} as effective_from
        {% for col in track_columns %},
        current.{{ col }},
        {{ col }}_effective.{{ col }}_effective_from
        {% endfor %}
    from current
    {% for col in track_columns %}
    left join {{ col }}_effective
        on {% for k in entity_partition %}current.{{ k }} = {{ col }}_effective.{{ k }} and {% endfor %}current.{{ col }} = {{ col }}_effective._val
    {% endfor %}
)

select * from final

{%- endmacro -%}
