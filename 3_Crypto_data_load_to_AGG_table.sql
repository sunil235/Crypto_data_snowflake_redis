use database demo_db;

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


--first load
insert into crypto_daily_AGG
select x.crypto_symbol,x.crypto_close,to_double(replace(x.crypto_change,'%','')) as crypto_change,min_crypto_close,max_crypto_close,x.starttime,current_timestamp,1 from
(select crypto_symbol,crypto_close,crypto_change,starttime,rank() over (partition by crypto_symbol order by starttime desc ) r from crypto_main)x,
(select crypto_symbol,min(crypto_close) min_crypto_close,max(crypto_close) max_crypto_close from crypto_main group by crypto_symbol )y
where x.crypto_symbol = y.crypto_symbol
and x.r =1
order by x.crypto_symbol

create or replace task load_crypto_daily_AGG
warehouse = COMPUTE_WH
SCHEDULE = '15 MINUTE'
AS 
merge into crypto_daily_AGG 
using
(
select x.crypto_symbol,x.crypto_close,to_double(replace(x.crypto_change,'%','')) as crypto_change,min_crypto_close,max_crypto_close,x.starttime from
(select crypto_symbol,crypto_close,crypto_change,starttime,rank() over (partition by crypto_symbol order by starttime desc ) r from crypto_main)x,
(select crypto_symbol,min(crypto_close) min_crypto_close,max(crypto_close) max_crypto_close from crypto_main group by crypto_symbol )y
where x.crypto_symbol = y.crypto_symbol
and x.r =1
and not exists (select 1 from crypto_daily_AGG y where x.crypto_symbol = y.symbol and x.starttime = y.starttime)  
--order by x.crypto_symbol
)delta
on
(
crypto_daily_AGG.symbol = delta.crypto_symbol
)
when matched then update set close = delta.crypto_close,pct_close_by_last=delta.crypto_change,min_close=delta.min_crypto_close,
max_close=delta.max_crypto_close,starttime=delta.starttime,upd_time=current_timestamp,upd_nu=upd_nu+1;

ALTER TASK IF EXISTS  load_crypto_daily_AGG RESUME;



---

ALTER TASK IF EXISTS  load_crypto_main_table SUSPEND;
ALTER TASK IF EXISTS  load_crypto_daily_AGG SUSPEND;


