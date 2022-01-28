
show streams
 show tasks
 
 select * from crypto_stage;
 select * from crypto_stream;
 select * from crypto_main order by 1 desc;
 select * from crypto_daily_AGG ;
 
 select *
  from table(information_schema.task_history())
  order by scheduled_time desc;
 
 select *
  from table(information_schema.task_history(
    scheduled_time_range_start=>dateadd('hour',-1,current_timestamp()),
    result_limit => 10,
    task_name=>'LOAD_CRYPTO_MAIN_TABLE'));
 
 
 