{% macro surrogate_key(fields) -%}
    to_hex(md5(concat(
        {%- for field in fields -%}
            coalesce(cast({{ field }} as string), '__null__')
            {%- if not loop.last %}, '|', {% endif -%}
        {%- endfor -%}
    )))
{%- endmacro %}

