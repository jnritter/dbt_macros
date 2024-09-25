{%- macro cleanse_date(date_column) -%}
    
	coalesce(try_cast(split({{ date_column }}, ' ')[0]::varchar as date), 
            date_from_parts(
                    try_cast(array_slice(split({{ date_column }}, '-'), -1, 3)[0]::varchar as int),
                    try_cast(array_slice(split({{ date_column }}, '-'), 0, 3)[0]::varchar as int),
                    try_cast(array_slice(split({{ date_column }}, '-'), -2, 3)[0]::varchar as int) 
                    )
            )

{%- endmacro -%}

{%- macro fix_date_years(date_column) -%}
    
    case 
        when year({{ date_column }}) < year(current_date()) - 100
            then case 
                    when right(year({{ date_column }})::varchar(100), 2)::int <= right(year(current_date())::varchar(100), 2)::int + 5
                        then dateadd('YEAR', 2000, {{ date_column }})
                    else dateadd('YEAR', 1900, {{ date_column }})
                end
        else {{ date_column }}
    end

{%- endmacro -%}

{%- macro cleanse_money(money_column) -%}
    
	try_cast({{ money_column }} as number(18,2))

{%- endmacro -%}

{%- macro cleanse_number(number_column) -%}
    
	try_cast({{ number_column }} as int)

{%- endmacro -%}

{%- macro make_lkp_text(id_column) -%}
    
	replace(replace(replace(lower({{ id_column }}), ' ', ''), '_', ''), '/', '')

{%- endmacro -%}

{%- macro cleanse_ssn(ssn_column) -%}
    
    case 
        when {{ ssn_column }} regexp '[0-9]{3}-[0-9]{2}-[0-9]{4}'
            then {{ ssn_column }}
        when try_cast({{ ssn_column }} as int) is not null
                and len({{ ssn_column }}) = 9
            then left({{ ssn_column }}, 3) || '-' || substr({{ ssn_column }}, 4, 2) || '-' || right({{ ssn_column }}, 4)
        else null
    end

{%- endmacro -%}

{%- macro is_valid_ssn(ssn_column) -%}
    case
        when {{ ssn_column }} is null
            then 1
        when try_cast(left({{ ssn_column }}, 3) as int) = 0
            then 2
        when try_cast(left({{ ssn_column }}, 3) as int) = 666
            then 3
        when try_cast(left({{ ssn_column }}, 3) as int) >= 900
            then 4
        when try_cast(split({{ ssn_column }}, '-')[2]::varchar as int) = 0
            then 5
        when try_cast(right({{ ssn_column }}, 4) as int) = 0
            then 6
        when
            {{ ssn_column }} in (
                '123-45-6789',
                '111-11-1111',
                '222-22-2222',
                '333-33-3333',
                '444-44-4444',
                '555-55-5555',
                '777-77-7777',
                '888-88-8888'
            )
            then 7
        else 0
    end = 0
{%- endmacro -%}
