SELECT match_key, game_id FROM
{{source('soka_prod', 'match_dim')}}