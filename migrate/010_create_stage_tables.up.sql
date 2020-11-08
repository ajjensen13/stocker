CREATE TABLE IF NOT EXISTS stage."Stocks"
(
    "Symbol"        TEXT                     NOT NULL
        CONSTRAINT stocks_pk
            PRIMARY KEY,
    "DisplaySymbol" TEXT DEFAULT ''::TEXT    NOT NULL,
    "Description"   TEXT                     NOT NULL,
    "Created"       timestamp with time zone NOT NULL,
    "Modified"      timestamp with time zone NOT NULL
);

CREATE TABLE IF NOT EXISTS stage."Candles"
(
    "Symbol"    TEXT                     NOT NULL
        CONSTRAINT candles_stocks_symbol_fk
            REFERENCES stage."Stocks",
    "Timestamp" TIMESTAMP WITH TIME ZONE NOT NULL,
    "Open"      REAL,
    "High"      REAL,
    "Low"       REAL,
    "Close"     REAL,
    "Volume"    REAL,
    "Created"   timestamp with time zone NOT NULL,
    "Modified"  timestamp with time zone NOT NULL,
    CONSTRAINT candles_pk
        PRIMARY KEY ("Symbol", "Timestamp")
);

CREATE TABLE IF NOT EXISTS stage."CompanyProfile"
(
    "Country"              TEXT                     NOT NULL,
    "Currency"             TEXT                     NOT NULL,
    "Exchange"             TEXT                     NOT NULL,
    "Name"                 TEXT                     NOT NULL,
    "Symbol"               TEXT                     NOT NULL,
    "Ipo"                  DATE,
    "MarketCapitalization" REAL                     NOT NULL,
    "SharesOutstanding"    REAL                     NOT NULL,
    "Logo"                 TEXT                     NOT NULL,
    "Phone"                TEXT                     NOT NULL,
    "WebUrl"               TEXT                     NOT NULL,
    "Industry"             TEXT                     NOT NULL,
    "Created"              timestamp with time zone NOT NULL,
    "Modified"             timestamp with time zone NOT NULL,
    CONSTRAINT companyprofile_pk
        PRIMARY KEY ("Exchange", "Symbol")
);