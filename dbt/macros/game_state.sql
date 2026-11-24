{#- Map a team-perspective goal difference to the 5-level game_state_key (-2 .. 2). -#}
{% macro game_state_key(goal_diff) -%}
    case
        when {{ goal_diff }} <= -2 then -2
        when {{ goal_diff }} >= 2 then 2
        else {{ goal_diff }}
    end
{%- endmacro %}
