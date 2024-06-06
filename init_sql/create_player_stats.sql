CREATE TABLE IF NOT EXISTS prod.player_stat (
    player_stat_key BIGSERIAL NOT NULL PRIMARY KEY,
    date_key BIGINT, 
    club_key BIGINT, 
    match_key BIGINT, 
    player_key BIGINT, 
    minute INT, 
    event_type VARCHAR(255), 
    event_description VARCHAR(255), 
    yellow_cards INT, 
    red_cards INT, 
    goals INT, 
    assists INT,
    created_at TIMESTAMP NOT NULL DEFAULT current_timestamp