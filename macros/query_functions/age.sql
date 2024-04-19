{%- macro calculate_age(birth_date_column, as_of_date_column=current_date()) -%}
    
    case 
        when dateadd(year, datediff(year, to_date(to_varchar({{ birth_date_column }})), {{ as_of_date_column }}), to_date({{ birth_date_column }})) > {{ as_of_date_column }}
            then datediff(year, to_date({{ birth_date_column }}), {{ as_of_date_column }}) - 1
        else datediff(year, to_date({{ birth_date_column }}), {{ as_of_date_column }})
    end

{%- endmacro -%}
