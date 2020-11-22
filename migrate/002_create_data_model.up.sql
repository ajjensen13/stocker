create schema if not exists metadata;
alter schema metadata owner to stocker;
alter default privileges in schema metadata grant select on tables to stocker_metadata_ro;
alter default privileges in schema metadata grant all on tables to stocker_metadata_rw;
create schema if not exists src;
alter schema src owner to stocker;
alter default privileges in schema src grant select on tables to stocker_src_ro;
alter default privileges in schema src grant all on tables to stocker_src_rw;
create schema if not exists stage;
alter schema stage owner to stocker;
alter default privileges in schema stage grant select on tables to stocker_stage_ro;
alter default privileges in schema stage grant all on tables to stocker_stage_rw;
create schema if not exists report;
alter schema report owner to stocker;
alter default privileges in schema report grant select on tables to stocker_report_ro;
alter default privileges in schema report grant all on tables to stocker_report_rw;


create table if not exists metadata.job_definition
(
    id   bigserial not null,
    name text      not null,
    constraint job_definition_pk
        primary key (id)
);

comment on table metadata.job_definition is 'represents a type of job that can be run periodically';

create table if not exists metadata.job_run
(
    id                bigserial                                          not null,
    created           timestamp with time zone default current_timestamp not null,
    modified          timestamp with time zone default current_timestamp not null,
    success           boolean,
    job_definition_id bigint,
    constraint job_run_pk
        primary key (id),
    constraint job_run_job_definition_id_fk
        foreign key (job_definition_id) references metadata.job_definition
            on delete set null
);

create table if not exists src.stocks
(
    symbol     text   not null,
    job_run_id bigint,
    data       jsonb,
    constraint stocks_pk
        primary key (job_run_id, symbol),
    constraint job_run_id_fk
        foreign key (job_run_id) references metadata.job_run
            on delete set null
);

comment on table src.stocks is 'Contains information about stocks as provided by finnhub';

create table if not exists src.candles
(
    symbol      text                      not null,
    "from"        timestamp with time zone  not null,
    "to"          timestamp with time zone  not null,
    job_run_id bigint,
    data       jsonb,
    constraint candles_pk
        primary key (job_run_id, symbol),
    constraint job_run_id_fk
        foreign key (job_run_id) references metadata.job_run
            on delete set null
);

comment on table src.candles is 'Contains daily stock candles as far back as provided by finnhub';

create table if not exists src.company_profiles
(
    ticker     text   not null,
    job_run_id bigint,
    data       jsonb,
    constraint company_profiles_pk
        primary key (job_run_id, ticker),
    constraint job_run_id_fk
        foreign key (job_run_id) references metadata.job_run
            on delete set null
);

comment on table src.company_profiles is 'Contains company profiles as provided by finnhub';

create table if not exists stage.stocks
(
    symbol         text                     not null,
    display_symbol text default ''::text    not null,
    description    text                     not null,
    created        timestamp with time zone not null,
    modified       timestamp with time zone not null,
    job_run_id     bigint,
    constraint stocks_pk
        primary key (symbol),
    constraint job_run_id_fk
        foreign key (job_run_id) references metadata.job_run
            on delete set null
);

comment on table stage.stocks is 'Contains staged information about stocks';

create table if not exists stage.candles
(
    symbol     text                     not null,
    timestamp  timestamp with time zone not null,
    open       real,
    high       real,
    low        real,
    close      real,
    volume     real,
    created    timestamp with time zone not null,
    modified   timestamp with time zone not null,
    job_run_id bigint,
    constraint candles_pk
        primary key (symbol, timestamp),
    constraint candles_stocks_symbol_fk
        foreign key (symbol) references stage.stocks,
    constraint job_run_id_fk
        foreign key (job_run_id) references metadata.job_run
            on delete set null
);

comment on table stage.candles is 'Contains staged daily stock candles';

create index if not exists ndx_candles_modified
    on stage.candles (modified);

create table if not exists stage.company_profiles
(
    country               text                     not null,
    currency              text                     not null,
    exchange              text                     not null,
    name                  text                     not null,
    symbol                text                     not null,
    ipo                   date,
    market_capitalization real                     not null,
    shares_outstanding    real                     not null,
    logo                  text                     not null,
    phone                 text                     not null,
    web_url               text                     not null,
    industry              text                     not null,
    created               timestamp with time zone not null,
    modified              timestamp with time zone not null,
    job_run_id            bigint,
    constraint company_profiles_pk
        primary key (symbol),
    constraint company_profiles_stocks_symbol_fk
        foreign key (symbol) references stage.stocks,
    constraint job_run_id_fk
        foreign key (job_run_id) references metadata.job_run
            on delete set null
);

comment on table stage.company_profiles is 'Contains staged company profiles';

create table if not exists stage.candles_52wk
(
    symbol               text                     not null,
    timestamp            timestamp with time zone not null,
    high_52wk            real,
    low_52wk             real,
    volume_52wk_avg      real,
    open                 real,
    high                 real,
    low                  real,
    close                real,
    volume               real,
    created              timestamp with time zone,
    modified             timestamp with time zone,
    timestamp_52wk_count integer,
    job_run_id            bigint,
    constraint stage_candles_pk
        primary key (symbol, timestamp),
    constraint candles_stocks_symbol_fk
        foreign key (symbol) references stage.stocks,
    constraint candles_52wk_candles_symbol_timestamp_fk
        foreign key (symbol, timestamp) references stage.candles
);

comment on table stage.candles_52wk is 'Contains staged 52 week high, low, and volume, statistics';

create unique index if not exists job_definition_name_uindex
    on metadata.job_definition (name);

create or replace view report.candles(symbol, timestamp, open, high, low, close, volume, created, modified) as
select candles.symbol,
       candles."timestamp",
       candles.open,
       candles.high,
       candles.low,
       candles.close,
       candles.volume,
       candles.created,
       candles.modified
from stage.candles;

comment on view report.candles is 'Exposing daily stock candle data for reporting';

create or replace view report.stocks(symbol, display_symbol, description, created, modified) as
select stocks.symbol,
       stocks.display_symbol,
       stocks.description,
       stocks.created,
       stocks.modified
from stage.stocks;

comment on view report.stocks is 'Exposes information about stocks for reporting';

create or replace view report.company_profiles
            (country, currency, exchange, name, symbol, ipo, market_capitalization, shares_outstanding, logo, phone,
             web_url, industry, created, modified)
as
select company_profiles.country,
       company_profiles.currency,
       company_profiles.exchange,
       company_profiles.name,
       company_profiles.symbol,
       company_profiles.ipo,
       company_profiles.market_capitalization,
       company_profiles.shares_outstanding,
       company_profiles.logo,
       company_profiles.phone,
       company_profiles.web_url as web_url,
       company_profiles.industry,
       company_profiles.created,
       company_profiles.modified
from stage.company_profiles;


create or replace view report.candles_52wk (symbol,
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
                                            timestamp_52wk_count)
as
select symbol,
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
       timestamp_52wk_count,
       job_run_id
from stage.candles_52wk;

comment on view report.company_profiles is 'Exposes company profile data for reporting';

create or replace view stage.calculate_candles_52wk
            (symbol, timestamp, open, high, low, close, volume, created, modified, high_52wk, low_52wk, volume_52wk_avg,
             timestamp_52wk_count)
as
select anchor.symbol,
       anchor.timestamp,
       anchor.open,
       anchor.high,
       anchor.low,
       anchor.close,
       anchor.volume,
       anchor.created,
       anchor.modified,
       max(lag.high)          as high_52wk,
       min(lag.low)           as low_52wk,
       avg(lag.volume)        as volume_52wk_avg,
       count(lag.timestamp) as timestamp_52wk_count
from stage.candles anchor
    join stage.candles lag on anchor.symbol = lag.symbol
where lag."timestamp" between anchor.timestamp - interval '52 weeks' and anchor.timestamp
group by anchor.symbol,
         anchor.timestamp,
         anchor.open,
         anchor.high,
         anchor.low,
         anchor.close,
         anchor.volume,
         anchor.created,
         anchor.modified;

