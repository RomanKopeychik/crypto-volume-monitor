CREATE TABLE IF NOT EXISTS crypto_volume (
    ts    TIMESTAMPTZ             NOT NULL,
    symbol       TEXT             NOT NULL,
    volume_base  DOUBLE PRECISION,
    volume_usdt  DOUBLE PRECISION,
    price        DOUBLE PRECISION,
    PRIMARY KEY (ts, symbol)
);