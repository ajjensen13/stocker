CREATE TABLE IF NOT EXISTS src."Stocks"
(
    "Symbol"        TEXT                  NOT NULL
        CONSTRAINT stocks_pk
            PRIMARY KEY,
    "DisplaySymbol" TEXT DEFAULT ''::TEXT NOT NULL,
    "Description"   TEXT                  NOT NULL
);