use demo_db;
use database demo_db;

--Load crypto_stage table from table stage 

--put file:///Users/Sunil/crypto_data.json @%crypto_stage/crypto_data/;

COPY INTO "DEMO_DB"."PUBLIC"."CRYPTO_STAGE" FROM @/crypto_data FILE_FORMAT = '"DEMO_DB"."PUBLIC"."CRYPTO_JSON_FORMAT"' ON_ERROR = 'ABORT_STATEMENT' PURGE = TRUE;
--select count(0) from "DEMO_DB"."PUBLIC"."CRYPTO_STAGE"

--COPY INTO "MARKET_DB"."PUBLIC"."CRYPTO_MAIN" FROM '@"MARKET_DB"."PUBLIC"."MY_S3_UNLOAD_STAGE"' FILE_FORMAT = '"MARKET_DB"."PUBLIC"."MY_CSV_FILE_UNLOAD_FORMAT"' ON_ERROR = 'ABORT_STATEMENT' PURGE = FALSE;
 --Insert into main table via task
 
  create or replace task load_crypto_main_table
  warehouse = COMPUTE_WH
  SCHEDULE = '1 MINUTE'
  WHEN
  SYSTEM$STREAM_HAS_DATA('CRYPTO_STREAM')
  AS 
  insert into crypto_main
  select crypto_seq.nextval,to_timestamp_ntz(replace(crypto_data:time,',',''),'mm/dd/yyyy hh24:mi:ss') as starttime,crypto_data:crypto_name as crypto_symbol,to_double(crypto_data:close) as crypto_close,crypto_data:change as crypto_change from CRYPTO_STREAM s
   where metadata$action = 'INSERT'
    and not exists (select 1 from crypto_main m where m.starttime = to_timestamp_ntz(replace(crypto_data:time,',',''),'mm/dd/yyyy hh24:mi:ss') and m.crypto_symbol = crypto_data:crypto_name)
    ;
 
  use role accountadmin;
  GRANT EXECUTE TASK ON ACCOUNT TO ROLE SYSADMIN;
   use role SYSADMIN;
   ALTER TASK IF EXISTS  load_crypto_main_table RESUME;
  
