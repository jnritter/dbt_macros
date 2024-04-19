{%- macro fix_scd_end_dates(source_table, partition_by) -%}

{# This macro is for fixing the start and end dates of a dbt snapshot that has been already been standardized into a date-delineated Type II SCD table. 
  Call it as a post-hook and let it work. 
  The partition_by parameter should be at the grain of the entities whose history is being tracked.
  There must be a column called start_date, and a column called end_date.
  #}
{%- set key_cols = partition_by | join(', ') -%}

{% set merge_statement %}

merge into {{ source_table }} dst using(
with new_start_date as (
    select
        {{ key_cols }},
        start_date,
        end_date,
        case
            when hash_key = lag(hash_key) over(partition by {{ key_cols }} order by start_date)
                then null
            else start_date
        end as start_date_new
    from {{ source_table }}
),

new_end_date as (
    select
        *,
        coalesce(dateadd(day, -1,
                        lag(start_date_new) over(partition by {{ key_cols }} order by start_date_new desc)),
                to_date('9999-12-31')
        ) as end_date_new
    from new_start_date 
)

select * from new_end_date

) src
    on {% for col in partition_by %}
    dst.{{ col }} = src.{{ col }} and 
    {%- endfor %} 
    dst.start_date = src.start_date
when matched and src.start_date_new is null then 
    delete
when matched then update set 
    dst.end_date = src.end_date_new

{% endset %}

{% if execute %}

  {% set results = run_query(merge_statement) %}

{% else %}

  {% set results = [] %}

{% endif %}

{%- endmacro -%}
