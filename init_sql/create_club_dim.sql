CREATE TABLE IF NOT EXISTS prod.club_dim (
    club_key BIGSERIAL NOT NULL PRIMARY KEY,
    club_id BIGINT NOT NULL,
    club_name VARCHAR(255) NOT NULL,
    stadium_name VARCHAR(255) NOT NULL,
    stadium_seats BIGINT,
    created_at TIMESTAMP NOT NULL DEFAULT current_timestamp
);