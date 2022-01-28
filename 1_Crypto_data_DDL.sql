--Read data from Redis to snowflake stage
use database demo_db;
create or replace table crypto_stage
( 
  crypto_data variant
);


--create stream on stage table 
create or replace stream crypto_stream on table crypto_stage;

--create crypto main table 
create or replace table crypto_main
(
  id integer,
  starttime timestamp,
  crypto_symbol string,
  crypto_close double,
  crypto_change string
) ;

--TBD--create daily crypto AGG table 

create or replace table crypto_daily_AGG
(
symbol string,
close double,
pct_close_by_last double,
min_close double,
max_close double,
starttime timestamp,  
upd_time timestamp,
upd_nu integer  
) cluster by (to_date(upd_time),symbol);

--create sequence 

create or replace sequence crypto_seq;
