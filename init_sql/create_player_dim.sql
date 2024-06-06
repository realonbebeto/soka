CREATE TABLE IF NOT EXISTS prod.player_dim (
    player_key BIGSERIAL NOT NULL PRIMARY KEY,
    player_id BIGINT NOT NULL,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    name VARCHAR(255),
    last_season INT,
    country_of_birth VARCHAR(255),
    city_of_birth VARCHAR(255),
    country_of_citizenship VARCHAR(255),
    date_of_birth DATE,
    foot VARCHAR(32)
    height_in_cm INT,
    agent_name VARCHAR(255),
    highest_market_value_in_eur BIGINT,
    date_of_valuation DATE,
    market_value_in_eur BIGINT,
    created_at TIMESTAMP NOT NULL DEFAULT current_timestamp
);
 
