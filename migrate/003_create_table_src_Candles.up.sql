CREATE TABLE IF NOT EXISTS src."Candles"
(
    "Symbol"    TEXT                     NOT NULL
        CONSTRAINT candles_stocks_symbol_fk
            REFERENCES src."Stocks",
    "Timestamp" TIMESTAMP WITH TIME ZONE NOT NULL,
    "Open"      REAL,
    "High"      REAL,
    "Low"       REAL,
    "Close"     REAL,
    "Volume"    REAL,
    CONSTRAINT candles_pk
        PRIMARY KEY ("Symbol", "Timestamp")
);