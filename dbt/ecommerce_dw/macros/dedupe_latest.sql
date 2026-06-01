{% macro dedupe_latest(relation, partition_by, order_by="updated_at desc, extracted_at desc, loaded_at desc") %}
    select *
    from {{ relation }}
    qualify row_number() over (
        partition by {{ partition_by }}
        order by {{ order_by }}
    ) = 1
{% endmacro %}

