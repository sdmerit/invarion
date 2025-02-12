---------------------------ARR table adjusted-----------------
create or replace transient table invarion_prod.prod.invarion_arr_table_adj as
(
select c.pivot_month, a.*,
case when c.pivot_month < last_day(a.standardenddate_adjusted) then a.totalordercost else 0 end arr_currency,
case when c.pivot_month = last_day(a.standardenddate_adjusted) then a.totalordercost else 0 end churn_arr_currency

from invarion_prod.staging.invarion_orders_stg a

left join (select distinct(last_day(ds)) pivot_month from whsoftware_prod.staging.whs_date_dim order by 1) c 
on c.pivot_month >= last_day(a.standardstartdate) and c.pivot_month < last_day(a.standardenddate_adjusted)

);

-----------------------------------Refunded ARR table
create or replace transient table invarion_prod.prod.invarion_refunded_arr_table_adj as
(
select c.pivot_month, a.*,
case when c.pivot_month = last_day(a.standardenddate) then a.totalordercost else 0 end refunded_arr_currency,
'Refunded' customer_classification

from invarion_prod.staging.invarion_refunded_orders_stg a

left join (select distinct(last_day(ds)) pivot_month from whsoftware_prod.staging.whs_date_dim order by 1) c 
on c.pivot_month = last_day(a.standardenddate)

where c.pivot_month is not null

);

alter table invarion_prod.prod.invarion_refunded_arr_table_adj add column first_order_date date;
update invarion_prod.prod.invarion_refunded_arr_table_adj t1
set t1.first_order_date = t2.first_order_date
from (select kw_number, min(orderdate) first_order_date 
from invarion_prod.staging.invarion_orders_stg
group by 1) t2
where t1.kw_number = t2.kw_number;
