create or replace procedure invarion_prod.staging.invarion_sp00_main()
returns number
language sql
as

declare

c1 cursor for (select distinct product from invarion_prod.staging.invarion_product_dim);

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
    
update invarion_prod.prod.invarion_arr_table_adj_final t1
set t1.transaction_source = t2.source, t1.connectedto = t2.connectedto
from  invarion_prod.staging.transactions_raw t2
where t1.ordernumber = t2.id;

update invarion_prod.prod.invarion_arr_table_adj_final t1
set t1.country_name = t1.countrycode
where upper(trim(t1.country_name)) is null;

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
drop table if exists invarion_prod.staging.invarion_refunded_orders_stg;
drop table if exists invarion_prod.staging.invarion_not_refunded_orders_stg;
drop table if exists invarion_prod.staging.invarion_orders_stg;
drop table if exists invarion_prod.staging.dup_charges;
drop table if exists invarion_prod.staging.dup_refunds;
    
return 1;
end;    
