COMMENT ON SCHEMA src
    IS 'Schema "src" contains unmodified source data';

COMMENT ON TABLE src."Candles"
    IS 'Table src."Candles" contains daily stock candles as far back as provided by finnhub';

COMMENT ON TABLE src."CompanyProfiles"
    IS 'Table src."CompanyProfiles" contains company profiles as provided by finnhub';

COMMENT ON TABLE src."Stocks"
    IS 'Table src."CompanyProfiles" contains information about stocks as provided by finnhub';

COMMENT ON VIEW src."Candles52Wk"
    IS 'View src."Candles52Wk" calculates 52 week high, low, and volume, statistics based on table src."Candles"';

COMMENT ON SCHEMA stage
    IS 'Schema "stage" contains prepped, cleaned, and pre-calculated source data';

COMMENT ON TABLE stage."Candles"
    IS 'Table stage."Candles" contains staged daily stock candles from table src."Candles"';

COMMENT ON TABLE stage."CompanyProfiles"
    IS 'Table stage."CompanyProfiles" contains staged company profiles from table src."CompanyProfiles"';

COMMENT ON TABLE stage."Stocks"
    IS 'Table stage."Stocks" contains staged information about stocks from table src."Stocks"';

COMMENT ON TABLE stage."Candles52Wk"
    IS 'Table stage."Candles52Wk" contains staged 52 week high, low, and volume, statistics as calculated by view src."Candles52Wk"';

COMMENT ON SCHEMA report
    IS 'Schema "report" contains views of data for reporting. It is an abstraction of the "stage" schema to make migrations easier.';

COMMENT ON VIEW report."Candles"
    IS 'View report."Candles" exposing daily stock candle data for reporting';

COMMENT ON VIEW report."CompanyProfiles"
    IS 'View report."CompanyProfiles" exposes company profile data for reporting';

COMMENT ON VIEW report."Stocks"
    IS 'View report."Stocks" exposes information about stocks for reporting';

COMMENT ON VIEW report."Candles52Wk"
    IS 'View report."Candles52Wk" exposes 52 week high, low, and volume, statistics for reporting';
