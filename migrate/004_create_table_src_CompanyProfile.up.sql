CREATE TABLE IF NOT EXISTS src."CompanyProfile"
(
    "Country"              TEXT NOT NULL,
    "Currency"             TEXT NOT NULL,
    "Exchange"             TEXT NOT NULL,
    "Name"                 TEXT NOT NULL,
    "Symbol"               TEXT NOT NULL,
    "Ipo"                  DATE,
    "MarketCapitalization" REAL NOT NULL,
    "SharesOutstanding"    REAL NOT NULL,
    "Logo"                 TEXT NOT NULL,
    "Phone"                TEXT NOT NULL,
    "WebUrl"               TEXT NOT NULL,
    "Industry"             TEXT NOT NULL,
    CONSTRAINT companyprofile_pk
        PRIMARY KEY ("Exchange", "Symbol")
);