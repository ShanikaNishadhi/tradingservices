-- Initialize database schema for new_coin_listings

CREATE TABLE IF NOT EXISTS new_coin_listings (
    id SERIAL PRIMARY KEY,
    coin VARCHAR(20) NOT NULL,
    market VARCHAR(50) NOT NULL,
    trading_start TIMESTAMP NOT NULL,
    source VARCHAR(50) NOT NULL,
    url TEXT,
    reported_at TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW()
);
