{% macro as_int(expression) -%}
  {%- if target.type == 'bigquery' -%}
    safe_cast(nullif({{ expression }}, '') as int64)
  {%- else -%}
    nullif({{ expression }}, '')::int
  {%- endif -%}
{%- endmacro %}

{% macro as_float(expression) -%}
  {%- if target.type == 'bigquery' -%}
    safe_cast(nullif({{ expression }}, '') as float64)
  {%- else -%}
    nullif({{ expression }}, '')::double precision
  {%- endif -%}
{%- endmacro %}

{% macro time_part(expression, position) -%}
  {%- if target.type == 'bigquery' -%}
    split({{ expression }}, ':')[safe_offset({{ position - 1 }})]
  {%- else -%}
    split_part({{ expression }}, ':', {{ position }})
  {%- endif -%}
{%- endmacro %}

{% macro postgres_post_hooks(hooks) -%}
  {%- if target.type == 'postgres' -%}
    {{ return(hooks) }}
  {%- else -%}
    {{ return([]) }}
  {%- endif -%}
{%- endmacro %}

{% macro postgres_indexes(indexes) -%}
  {%- if target.type == 'postgres' -%}
    {{ return(indexes) }}
  {%- else -%}
    {{ return([]) }}
  {%- endif -%}
{%- endmacro %}

{% macro bigquery_config(config_value) -%}
  {%- if target.type == 'bigquery' -%}
    {{ return(config_value) }}
  {%- else -%}
    {{ return(none) }}
  {%- endif -%}
{%- endmacro %}

{% macro join_non_null(separator, expressions) -%}
  {%- if target.type == 'bigquery' -%}
    array_to_string(
      array(
        select value
        from unnest([
          {%- for expression in expressions -%}
            {{ expression }}{{ "," if not loop.last }}
          {%- endfor -%}
        ]) as value
        where value is not null
      ),
      '{{ separator }}'
    )
  {%- else -%}
    concat_ws(
      '{{ separator }}',
      {%- for expression in expressions -%}
        {{ expression }}{{ "," if not loop.last }}
      {%- endfor -%}
    )
  {%- endif -%}
{%- endmacro %}
