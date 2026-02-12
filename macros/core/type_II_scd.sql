{%- macro fix_scd_end_dates(source_table, partition_by, row_contents_hash='hash_key', start_date_col='start_date', end_date_col='end_date') -%}

{# This macro is for fixing the start and end dates of a dbt snapshot that has been already been standardized into a date-delineated Type II SCD table. 
  Call it as a post-hook and let it work. 
  The entity_key parameter should be at the grain of the entities whose history is being tracked.
  #}
{%- set key_cols = partition_by | join(', ') -%}

{% set merge_statement %}

merge into {{ source_table }} dst using (
with new_{{ start_date_col }} as (
    select
        {{ key_cols }},
        {{ start_date_col }},
        {{ end_date_col }},
        case
            when {{ row_contents_hash }} = lag({{ row_contents_hash }}) over(partition by {{ key_cols }} order by {{ start_date_col }})
                then null
            else {{ start_date_col }}
        end as {{ start_date_col }}_new
    from {{ source_table }}
),

new_{{ end_date_col }} as (
    select
        *,
        coalesce(dateadd(day, -1,
                        lag({{ start_date_col }}_new) over(partition by {{ key_cols }} order by {{ start_date_col }}_new desc)),
                to_date('9999-12-31')
        ) as {{ end_date_col }}_new
    from new_{{ start_date_col }} 
)

select * from new_{{ end_date_col }}

) src
    on {% for col in partition_by %}
    dst.{{ col }} = src.{{ col }} and 
    {%- endfor %} 
    dst.{{ start_date_col }} = src.{{ start_date_col }}
when matched and src.{{ start_date_col }}_new is null then 
    delete
when matched then update set 
    dst.{{ end_date_col }} = src.{{ end_date_col }}_new

{% endset %}

{% if execute %}

  {% set results = run_query(merge_statement) %}

{% else %}

  {% set results = [] %}

{% endif %}


{%- macro snapshot_to_scd2(entity_key, snapshot_ref, valid_from_date_col_names=['dbt_valid_from','start_date'], valid_to_date_col_names=['dbt_valid_to','end_date'], end_date_default='9999-12-31') -%}

{# This macro is for standardizing a dbt snapshot into a date-delineated Type II SCD table. 
  Call this instead of a normal ref() call in your model. 
  The entity_key parameter should be at the grain of the entities whose history is being tracked.
  The snapshot_ref parameter should be the name of the snapshot table.
  The valid_from_date_col_names parameter should be the name of the column that contains the valid from date.
  The valid_to_date_col_names parameter should be the name of the column that contains the valid to date.
  The end_date_default parameter should be the default value for the valid to date.
  #}

  select
    to_date({{ valid_from_date_col_names[0] }}) as {{ valid_from_date_col_names[1] }},
    coalesce(dateadd(day, -1, lag({{ valid_from_date_col_names[1] }}) over(partition by {{ entity_key }} order by {{ valid_from_date_col_names[1] }} desc)), to_date('{{ end_date_default }}')) as {{ valid_to_date_col_names[1] }},
    * except ({{ valid_from_date_col_names[0] }}, {{ valid_to_date_col_names[0] }})
  from {{ snapshot_ref }}
  qualify row_number() over(partition by {{ entity_key }}, to_date({{ valid_from_date_col_names[0] }}) order by {{ valid_from_date_col_names[0] }} desc) = 1

{%- endmacro -%}
