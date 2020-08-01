alter table src."Candles"
    add "Created"  timestamp with time zone not null default CURRENT_TIMESTAMP,
    add "Modified" timestamp with time zone not null default CURRENT_TIMESTAMP
;

update src."Candles"
set "Created"  = CURRENT_TIMESTAMP,
    "Modified" = CURRENT_TIMESTAMP
;

alter table src."Candles"
    alter column "Created" drop default,
    alter column "Modified" drop default
;

alter table src."CompanyProfile"
    add "Created"  timestamp with time zone not null default CURRENT_TIMESTAMP,
    add "Modified" timestamp with time zone not null default CURRENT_TIMESTAMP
;

update src."CompanyProfile"
set "Created"  = CURRENT_TIMESTAMP,
    "Modified" = CURRENT_TIMESTAMP
;

alter table src."CompanyProfile"
    alter column "Created" drop default,
    alter column "Modified" drop default
;

alter table src."Stocks"
    add "Created"  timestamp with time zone not null default CURRENT_TIMESTAMP,
    add "Modified" timestamp with time zone not null default CURRENT_TIMESTAMP
;

update src."Stocks"
set "Created"  = CURRENT_TIMESTAMP,
    "Modified" = CURRENT_TIMESTAMP
;

alter table src."Stocks"
    alter column "Created" drop default,
    alter column "Modified" drop default
;