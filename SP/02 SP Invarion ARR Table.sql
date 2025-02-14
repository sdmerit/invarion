create or replace procedure invarion_prod.staging.invarion_sp02_load_arr()
returns number
language sql
as

begin

create or replace transient table invarion_prod.prod.invarion_arr_table_adj_pg as
(
select c.pivot_month, a.*,
case when c.pivot_month < last_day(a.standardenddate_adjusted) then a.totalordercost else 0 end arr_currency,
case when c.pivot_month = last_day(a.standardenddate_adjusted) then a.totalordercost else 0 end churn_arr_currency

from invarion_prod.staging.invarion_orders_stg_pg a

left join (select distinct(last_day(ds)) pivot_month from whsoftware_prod.staging.whs_date_dim order by 1) c 
on c.pivot_month >= last_day(a.standardstartdate) and c.pivot_month < last_day(a.standardenddate_adjusted)

);

return 1;

end;