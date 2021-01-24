CREATE ROLE stocker_ro
;

CREATE SCHEMA IF NOT EXISTS metadata
;

ALTER SCHEMA metadata OWNER TO stocker
;

ALTER DEFAULT PRIVILEGES IN SCHEMA metadata GRANT SELECT ON TABLES TO stocker_ro
;

CREATE SCHEMA IF NOT EXISTS src
;

ALTER SCHEMA src OWNER TO stocker
;

ALTER DEFAULT PRIVILEGES IN SCHEMA src GRANT SELECT ON TABLES TO stocker_ro
;

CREATE SCHEMA IF NOT EXISTS stage
;

ALTER SCHEMA stage OWNER TO stocker
;

ALTER DEFAULT PRIVILEGES IN SCHEMA stage GRANT SELECT ON TABLES TO stocker_ro
;

CREATE SCHEMA IF NOT EXISTS report
;

ALTER SCHEMA report OWNER TO stocker
;

ALTER DEFAULT PRIVILEGES IN SCHEMA report GRANT SELECT ON TABLES TO stocker_ro
;

CREATE TABLE IF NOT EXISTS metadata.job_definition (
    id   bigserial NOT NULL,
    name text      NOT NULL,
    CONSTRAINT job_definition_pk
        PRIMARY KEY (id)
)
;

COMMENT ON TABLE metadata.job_definition IS 'Represents a type of job that can be run periodically'
;

CREATE TABLE IF NOT EXISTS metadata.job_run (
    job_definition_id bigint,
    id                bigserial                                          NOT NULL,
    created           timestamp WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    modified          timestamp WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
    success           boolean,
    CONSTRAINT job_run_pk
        PRIMARY KEY (id),
    CONSTRAINT job_run_job_definition_id_fk
        FOREIGN KEY (job_definition_id)
            REFERENCES metadata.job_definition
            ON DELETE SET NULL
)
;

CREATE TABLE IF NOT EXISTS src.stocks (
    job_run_id bigint,
    symbol     text NOT NULL,
    data       jsonb,
    CONSTRAINT stocks_pk
        PRIMARY KEY (job_run_id, symbol),
    CONSTRAINT job_run_id_fk
        FOREIGN KEY (job_run_id)
            REFERENCES metadata.job_run
            ON DELETE SET NULL
)
;

COMMENT ON TABLE src.stocks IS 'Contains information about stocks as provided by finnhub'
;

CREATE TABLE IF NOT EXISTS src.candles (
    job_run_id bigint,
    symbol     text                     NOT NULL,
    "from"     timestamp WITH TIME ZONE NOT NULL,
    "to"       timestamp WITH TIME ZONE NOT NULL,
    data       jsonb,
    CONSTRAINT candles_pk
        PRIMARY KEY (job_run_id, symbol),
    CONSTRAINT job_run_id_fk
        FOREIGN KEY (job_run_id)
            REFERENCES metadata.job_run
            ON DELETE SET NULL
)
;

COMMENT ON TABLE src.candles IS 'Contains daily stock candles as far back as provided by finnhub'
;

CREATE TABLE IF NOT EXISTS src.company_profiles (
    job_run_id bigint,
    symbol     text NOT NULL,
    data       jsonb,
    CONSTRAINT company_profiles_pk
        PRIMARY KEY (job_run_id, symbol),
    CONSTRAINT job_run_id_fk
        FOREIGN KEY (job_run_id)
            REFERENCES metadata.job_run
            ON DELETE SET NULL
)
;

COMMENT ON TABLE src.company_profiles IS 'Contains company profiles as provided by finnhub'
;

CREATE TABLE IF NOT EXISTS stage.stocks (
    job_run_id     bigint,
    symbol         text                     NOT NULL,
    display_symbol text DEFAULT ''::text    NOT NULL,
    description    text                     NOT NULL,
    created        timestamp WITH TIME ZONE NOT NULL,
    modified       timestamp WITH TIME ZONE NOT NULL,
    CONSTRAINT stocks_pk
        PRIMARY KEY (symbol),
    CONSTRAINT job_run_id_fk
        FOREIGN KEY (job_run_id)
            REFERENCES metadata.job_run
            ON DELETE SET NULL
)
;

COMMENT ON TABLE stage.stocks IS 'Contains staged information about stocks'
;

CREATE TABLE IF NOT EXISTS stage.candles (
    job_run_id bigint,
    symbol     text                     NOT NULL,
    timestamp  timestamp WITH TIME ZONE NOT NULL,
    open       real,
    high       real,
    low        real,
    close      real,
    volume     real,
    created    timestamp WITH TIME ZONE NOT NULL,
    modified   timestamp WITH TIME ZONE NOT NULL,
    CONSTRAINT candles_pk
        PRIMARY KEY (symbol, timestamp),
    CONSTRAINT candles_stocks_symbol_fk
        FOREIGN KEY (symbol)
            REFERENCES stage.stocks,
    CONSTRAINT job_run_id_fk
        FOREIGN KEY (job_run_id)
            REFERENCES metadata.job_run
            ON DELETE SET NULL
)
;

COMMENT ON TABLE stage.candles IS 'Contains staged daily stock candles'
;

CREATE INDEX IF NOT EXISTS ndx_candles_modified
    ON stage.candles (modified)
;

CREATE TABLE IF NOT EXISTS stage.company_profiles (
    job_run_id            bigint,
    symbol                text                     NOT NULL,
    country               text                     NOT NULL,
    currency              text                     NOT NULL,
    exchange              text                     NOT NULL,
    name                  text                     NOT NULL,
    ticker                text                     NOT NULL,
    ipo                   date,
    market_capitalization real                     NOT NULL,
    shares_outstanding    real                     NOT NULL,
    logo                  text                     NOT NULL,
    phone                 text                     NOT NULL,
    web_url               text                     NOT NULL,
    industry              text                     NOT NULL,
    created               timestamp WITH TIME ZONE NOT NULL,
    modified              timestamp WITH TIME ZONE NOT NULL,
    CONSTRAINT company_profiles_pk
        PRIMARY KEY (symbol),
    CONSTRAINT stocks_symbol_fk
        FOREIGN KEY (symbol)
            REFERENCES stage.stocks,
    CONSTRAINT job_run_id_fk
        FOREIGN KEY (job_run_id)
            REFERENCES metadata.job_run
            ON DELETE SET NULL
)
;

COMMENT ON TABLE stage.company_profiles IS 'Contains staged company profiles'
;

CREATE TABLE IF NOT EXISTS stage.candles_52wk (
    job_run_id           bigint,
    symbol               text                     NOT NULL,
    timestamp            timestamp WITH TIME ZONE NOT NULL,
    high_52wk            real,
    low_52wk             real,
    volume_52wk_avg      real,
    open                 real,
    high                 real,
    low                  real,
    close                real,
    volume               real,
    created              timestamp WITH TIME ZONE,
    modified             timestamp WITH TIME ZONE,
    timestamp_52wk_count integer,
    CONSTRAINT candles_52wk_pk
        PRIMARY KEY (symbol, timestamp),
    CONSTRAINT candles_stocks_symbol_fk
        FOREIGN KEY (symbol)
            REFERENCES stage.stocks,
    CONSTRAINT candles_symbol_timestamp_fk
        FOREIGN KEY (symbol, timestamp)
            REFERENCES stage.candles
)
;

COMMENT ON TABLE stage.candles_52wk IS 'Contains staged 52 week high, low, and volume, statistics'
;

CREATE UNIQUE INDEX IF NOT EXISTS job_definition_name_uindex
    ON metadata.job_definition (name)
;

CREATE OR REPLACE VIEW report.candles(symbol, timestamp, open, high, low, close, volume, created, modified) AS
    SELECT candles.symbol,
           candles."timestamp",
           candles.open,
           candles.high,
           candles.low,
           candles.close,
           candles.volume,
           candles.created,
           candles.modified
    FROM stage.candles
;

COMMENT ON VIEW report.candles IS 'Exposing daily stock candle data for reporting'
;

CREATE OR REPLACE VIEW report.stocks(symbol, display_symbol, description, created, modified) AS
    SELECT stocks.symbol,
           stocks.display_symbol,
           stocks.description,
           stocks.created,
           stocks.modified
    FROM stage.stocks
;

COMMENT ON VIEW report.stocks IS 'Exposes information about stocks for reporting'
;

CREATE OR REPLACE VIEW report.company_profiles
            (symbol, country, currency, exchange, name, ticker, ipo, market_capitalization, shares_outstanding, logo,
             phone, web_url, industry, created, modified)
AS
    SELECT company_profiles.symbol,
           company_profiles.country,
           company_profiles.currency,
           company_profiles.exchange,
           company_profiles.name,
           company_profiles.ticker,
           company_profiles.ipo,
           company_profiles.market_capitalization,
           company_profiles.shares_outstanding,
           company_profiles.logo,
           company_profiles.phone,
           company_profiles.web_url AS web_url,
           company_profiles.industry,
           company_profiles.created,
           company_profiles.modified
    FROM stage.company_profiles
;


CREATE OR REPLACE VIEW report.candles_52wk
            (symbol, timestamp, high_52wk, low_52wk, volume_52wk_avg, open, high, low, close, volume, created, modified,
             timestamp_52wk_count)
AS
    SELECT symbol,
           timestamp,
           high_52wk,
           low_52wk,
           volume_52wk_avg,
           open,
           high,
           low,
           close,
           volume,
           created,
           modified,
           timestamp_52wk_count
    FROM stage.candles_52wk
;

COMMENT ON VIEW report.company_profiles IS 'Exposes company profile data for reporting'
;

CREATE OR REPLACE VIEW stage.calculate_candles_52wk
            (symbol, timestamp, open, high, low, close, volume, created, modified, high_52wk, low_52wk, volume_52wk_avg,
             timestamp_52wk_count)
AS
    SELECT anchor.symbol,
           anchor.timestamp,
           anchor.open,
           anchor.high,
           anchor.low,
           anchor.close,
           anchor.volume,
           anchor.created,
           anchor.modified,
           MAX(lag.high)        AS high_52wk,
           MIN(lag.low)         AS low_52wk,
           AVG(lag.volume)      AS volume_52wk_avg,
           COUNT(lag.timestamp) AS timestamp_52wk_count
    FROM stage.candles anchor
        JOIN stage.candles lag
        ON anchor.symbol = lag.symbol
    WHERE lag."timestamp" BETWEEN anchor.timestamp - INTERVAL '52 weeks' AND anchor.timestamp
    GROUP BY anchor.symbol,
             anchor.timestamp,
             anchor.open,
             anchor.high,
             anchor.low,
             anchor.close,
             anchor.volume,
             anchor.created,
             anchor.modified
;

INSERT INTO
    metadata.job_definition (name)
VALUES ('Finnhub ETL')
;