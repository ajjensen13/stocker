DROP VIEW IF EXISTS report."Candles";
CREATE VIEW report."Candles" AS
SELECT "Symbol",
       "Timestamp",
       "Open",
       "High",
       "Low",
       "Close",
       "Volume",
       "Created",
       "Modified"
FROM stage."Candles";

DROP VIEW IF EXISTS report."Stocks";
CREATE VIEW report."Stocks" AS
SELECT "Symbol",
       "DisplaySymbol",
       "Description",
       "Created",
       "Modified"
FROM stage."Stocks";

DROP VIEW IF EXISTS report."CompanyProfiles";
CREATE VIEW report."CompanyProfiles" AS
SELECT "Country",
       "Currency",
       "Exchange",
       "Name",
       "Symbol",
       "Ipo",
       "MarketCapitalization",
       "SharesOutstanding",
       "Logo",
       "Phone",
       "WebUrl",
       "Industry",
       "Created",
       "Modified"
FROM stage."CompanyProfiles";