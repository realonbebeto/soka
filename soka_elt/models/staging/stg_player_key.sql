SELECT player_key, player_id FROM
{{source('soka_prod', 'player_dim')}}