{% test not_negative(model, column_name) %}
    -- Test that ensures column values are not negative
    select *
    from {{ model }}
    where {{ column_name }} < 0
{% endtest %}