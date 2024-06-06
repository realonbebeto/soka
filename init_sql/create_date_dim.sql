CREATE TABLE IF NOT EXISTS prod.date_dim (
    date_key BIGINT PRIMARY KEY,
    date DATE NOT NULL,
    date_description VARCHAR(255) NOT NULL,
    day_of_week VARCHAR(64) NOT NULL,
    day_number_in_calendar_month INT NOT NULL,
    day_number_in_calendar_year INT NOT NULL,
    calendar_week_number_in_year INT NOT NULL,
    calendar_month_name VARCHAR(64) NOT NULL,
    calendar_month_number_in_year INT NOT NULL,
    calendar_year_month VARCHAR(64) NOT NULL,
    calendar_year INT NOT NULL,
    weekday_indicator VARCHAR(255),
    created_at TIMESTAMP NOT NULL DEFAULT current_timestamp
);