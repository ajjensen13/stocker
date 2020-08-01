alter table src."Candles"
    add "Created"  timestamp with time zone not null,
    add "Modified" timestamp with time zone not null
;

update src."Candles"
set "Created"  = CURRENT_TIMESTAMP,
    "Modified" = CURRENT_TIMESTAMP
;

alter table src."CompanyProfile"
    add "Created"  timestamp with time zone not null,
    add "Modified" timestamp with time zone not null
;

update src."CompanyProfile"
set "Created"  = CURRENT_TIMESTAMP,
    "Modified" = CURRENT_TIMESTAMP
;

alter table src."Stocks"
    add "Created"  timestamp with time zone not null,
    add "Modified" timestamp with time zone not null
;

update src."Stocks"
set "Created"  = CURRENT_TIMESTAMP,
    "Modified" = CURRENT_TIMESTAMP
;