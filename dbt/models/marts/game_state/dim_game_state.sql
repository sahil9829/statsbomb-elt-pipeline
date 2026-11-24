-- Game-state dimension: five detailed states rolled up into three, with sort orders
-- so BI tools show Losing -> Drawing -> Winning instead of alphabetically, and a
-- diverging red-grey-blue colour per state (validated for colour-vision deficiency)
-- so every chart colours a state the same way via conditional formatting.
select * from (
    values
        (-2, 'Losing by 2+', 'Losing', 1, 1, '#9E1B2B', '#D6604D'),
        (-1, 'Losing by 1', 'Losing', 2, 1, '#D6604D', '#D6604D'),
        (0, 'Drawing', 'Drawing', 3, 2, '#ADADAD', '#8E8E8E'),
        (1, 'Winning by 1', 'Winning', 4, 3, '#4A90C8', '#2E78B7'),
        (2, 'Winning by 2+', 'Winning', 5, 3, '#1F5294', '#2E78B7')
) as t (
    game_state_key, game_state_detail, game_state, game_state_detail_sort, game_state_sort,
    game_state_detail_color, game_state_color
)
