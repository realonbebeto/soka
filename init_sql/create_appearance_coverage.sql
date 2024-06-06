CREATE TABLE IF NOT EXISTS prod.appearance_coverage (
    appearance_key BIGSERIAL NOT NULL PRIMARY KEY,
    date_id BIGINT, 
    club_id BIGINT, 
    match_id BIGINT, 
    player_id BIGINT, 
    type VARCHAR(255), 
    position VARCHAR(255),
    team_captain INT, 
    minutes_played INT,
    created_at TIMESTAMP NOT NULL DEFAULT current_timestamp
);