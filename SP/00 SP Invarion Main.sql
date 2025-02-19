create or replace procedure invarion_prod.staging.invarion_sp00_main()
returns number
language sql
as

declare

c1 cursor for (select distinct case objecttypeid
when 'dce29b08-10f0-48d2-b21e-b047c3b29865' then 'RapidPlan'
when '46d65d68-013d-457a-8918-6cb09c93795b' then 'RapidPlan Training'
when '89294981-6053-4308-8d75-add1c2d2b1df' then 'RapidPath'
when 'e9d3b428-6582-45fe-96bc-9b52e6ea4578' then 'RapidPath Training'
when 'bfbfb16b-bc0b-4993-a221-c5e3a87410b6' then 'RapidPlan Online'
when '92a5e8e7-ece4-43c4-8ce4-526fbb7272e0' then 'RapidPath Online'
else 'Addon' end product from invarion_prod.staging.classified_transactions);

tmp_product string := null;

begin

    call invarion_prod.staging.invarion_sp01_load_staging('all');
    create or replace transient table invarion_prod.staging.invarion_orders_stg as 
        select * from invarion_prod.staging.invarion_orders_stg_pg;
    call invarion_prod.staging.invarion_sp02_load_arr();
    call invarion_prod.staging.invarion_sp03_load_arrrollforward();
    create or replace transient table invarion_prod.prod.invarion_arr_table_adj_final as
        select 'All' for_product, * from invarion_prod.prod.invarion_arr_table_adj_final_pg;
    create or replace transient table invarion_prod.prod.invarion_arr_rollforward_adj as
        select 'All' for_product, * from invarion_prod.prod.invarion_arr_rollforward_adj_pg; 

    open c1;
    for record in c1 do
        tmp_product := record.product;
        call invarion_prod.staging.invarion_sp01_load_staging(:tmp_product);
        call invarion_prod.staging.invarion_sp02_load_arr();
        call invarion_prod.staging.invarion_sp03_load_arrrollforward();
        create or replace transient table invarion_prod.prod.invarion_arr_table_adj_final as
            select * from invarion_prod.prod.invarion_arr_table_adj_final
            union all
            select :tmp_product as for_product, * from invarion_prod.prod.invarion_arr_table_adj_final_pg;
            
        create or replace transient table invarion_prod.prod.invarion_arr_rollforward_adj as
            select * from invarion_prod.prod.invarion_arr_rollforward_adj
            union all
            select :tmp_product as for_product, * from invarion_prod.prod.invarion_arr_rollforward_adj_pg;    
        
    end for;
    close c1;

drop table if exists invarion_prod.prod.invarion_arr_table_adj_pg;
drop table if exists invarion_prod.staging.invarion_upg_dwg_ref_adj_pg;
drop table if exists INVARION_PROD.PROD.INVARION_ARR_ROLLFORWARD_ADJ_PG;
drop table if exists INVARION_PROD.PROD.INVARION_ARR_TABLE_ADJ;
drop table if exists INVARION_PROD.PROD.INVARION_ARR_TABLE_ADJ_FINAL_PG;
drop table if exists INVARION_PROD.PROD.INVARION_ARR_TABLE_ADJ_PG;
drop table if exists INVARION_PROD.STAGING.INVARION_ORDERS_STG_PG;
drop table if exists INVARION_PROD.STAGING.INVARION_REGISTRATION_DATES_ADJ;
drop table if exists INVARION_PROD.STAGING.INVARION_REGISTRATION_DATES_ADJ_PG;
drop table if exists invarion_prod.staging.refunded_transactions_mapping;
drop table if exists invarion_prod.staging.not_refunded_transactions;
    
return 1;
end;    
