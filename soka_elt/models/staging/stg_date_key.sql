SELECT date_key, date FROM
{{source('soka_prod', 'date_dim')}}