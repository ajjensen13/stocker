CREATE TABLE IF NOT EXISTS stage."Candles52Wk"
(
    "Symbol"      TEXT                     NOT NULL
        CONSTRAINT candles_stocks_symbol_fk
            REFERENCES stage."Stocks",
    "Timestamp"   TIMESTAMP WITH TIME ZONE NOT NULL,
    "HighMax"     real,
    "HighMin"     real,
    "HighAvg"     real,
    "HighCount"   integer,
    "LowMax"      real,
    "LowMin"      real,
    "LowAvg"      real,
    "LowCount"    integer,
    "VolumeMax"   real,
    "VolumeMin"   real,
    "VolumeAvg"   real,
    "VolumeCount" integer,
    CONSTRAINT stage_candles_pk
        PRIMARY KEY ("Symbol", "Timestamp")
);