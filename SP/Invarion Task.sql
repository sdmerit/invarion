create or replace task invarion_prod.staging.invarion_t01_arr_process
warehouse = MERIT_INVARION_WH
schedule = 'USING CRON 55 15 * * * UTC'
as
begin

    call invarion_prod.staging.invarion_sp00_main();
    
end;

alter task invarion_prod.staging.invarion_t01_arr_process resume;