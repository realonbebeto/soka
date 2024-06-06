SELECT club_key, club_id FROM
{{source('soka_prod', 'club_dim')}}