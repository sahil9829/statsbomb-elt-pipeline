{#-
  StatsBomb pitch coordinates are 120 x 80 yards. Rows outside the pitch indicate
  a parsing bug (e.g. x/y swapped) rather than a real event. Nulls are allowed.
-#}
{% test pitch_coordinate(model, column_name, axis) %}
    {%- set max_value = 120 if axis == 'x' else 80 -%}
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} is not null
      and ({{ column_name }} < 0 or {{ column_name }} > {{ max_value }})
{% endtest %}
