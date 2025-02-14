create or replace procedure invarion_prod.staging.invarion_sp01_load_staging(sp_product string default 'all')
returns number
language sql
as

declare
tmp_product string := null;

begin

let grace_period integer := 0;
tmp_product := sp_product;

if (sp_product = 'all') then

create or replace transient table invarion_prod.staging.invarion_orders_stg_pg as
(select a.currentbillingentityid customer_id, a.id ordernumber, date(CONVERT_TIMEZONE('UTC', a."DATE")) orderdate, 
round(a.transactionarr,4) totalordercost, 
a.termlength subtypeatordertime,
upper(trim(a.customername)) company,
upper(trim(b.classification)) company_type,
upper(trim(b.countrycode)) countrycode, 
upper(trim(b.state)) state, 
upper(trim(b.city)) city,

case a.objecttypeid
when 'dce29b08-10f0-48d2-b21e-b047c3b29865' then 'RapidPlan'
when '46d65d68-013d-457a-8918-6cb09c93795b' then 'RapidPlan Training'
when '89294981-6053-4308-8d75-add1c2d2b1df' then 'RapidPath'
when 'e9d3b428-6582-45fe-96bc-9b52e6ea4578' then 'RapidPath Training'
when 'bfbfb16b-bc0b-4993-a221-c5e3a87410b6' then 'RapidPlan Online'
when '92a5e8e7-ece4-43c4-8ce4-526fbb7272e0' then 'RapidPath Online'
else 'Addon' end product,

orderdate standardstartdate,
date(CONVERT_TIMEZONE('UTC', a.periodend)) standardenddate

from invarion_prod.staging.classified_transactions a
left join invarion_prod.staging.company b on b.id = a.currentbillingentityid

where a.transactionarr > 0 
and nvl(lower(a.type),'-') not in ('refund') 
and a.matchingrefund not in (select distinct id from invarion_prod.staging.classified_transactions where nvl(lower(a.type),'-') in ('refund') )
order by a.currentbillingentityid, a.id, orderdate
);

---------------Load refund tables
create or replace transient table invarion_prod.staging.invarion_refunded_orders_stg as
(select a.currentbillingentityid customer_id, a.id ordernumber, date(CONVERT_TIMEZONE('UTC', a."DATE")) orderdate,
round(a.transactionarr,4) totalordercost, 
a.termlength subtypeatordertime,
upper(trim(a.customername)) company,
upper(trim(b.classification)) company_type,
upper(trim(b.countrycode)) countrycode, 
upper(trim(b.state)) state, 
upper(trim(b.city)) city,

case a.objecttypeid
when 'dce29b08-10f0-48d2-b21e-b047c3b29865' then 'RapidPlan'
when '46d65d68-013d-457a-8918-6cb09c93795b' then 'RapidPlan Training'
when '89294981-6053-4308-8d75-add1c2d2b1df' then 'RapidPath'
when 'e9d3b428-6582-45fe-96bc-9b52e6ea4578' then 'RapidPath Training'
when 'bfbfb16b-bc0b-4993-a221-c5e3a87410b6' then 'RapidPlan Online'
when '92a5e8e7-ece4-43c4-8ce4-526fbb7272e0' then 'RapidPath Online'
else 'Addon' end product,

orderdate standardstartdate,
date(CONVERT_TIMEZONE('UTC', a.periodend)) standardenddate

from invarion_prod.staging.classified_transactions a
left join invarion_prod.staging.company b on b.id = a.currentbillingentityid

where a.transactionarr > 0 
and a.matchingrefund in (select distinct id from invarion_prod.staging.classified_transactions where nvl(lower(type),'-') in ('refund') )
order by a.currentbillingentityid, a.id, orderdate
);

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
from (select customer_id, min(orderdate) first_order_date 
from invarion_prod.staging.invarion_orders_stg_pg
group by 1) t2
where t1.customer_id = t2.customer_id;

-------------------------------------------------------
else 

create or replace transient table invarion_prod.staging.invarion_orders_stg_pg as
(select a.currentbillingentityid customer_id, a.id ordernumber, date(CONVERT_TIMEZONE('UTC', a."DATE")) orderdate, 
round(a.transactionarr,4) totalordercost, 
a.termlength subtypeatordertime,
upper(trim(a.customername)) company,
upper(trim(b.classification)) company_type,
upper(trim(b.countrycode)) countrycode, 
upper(trim(b.state)) state, 
upper(trim(b.city)) city,

case a.objecttypeid
when 'dce29b08-10f0-48d2-b21e-b047c3b29865' then 'RapidPlan'
when '46d65d68-013d-457a-8918-6cb09c93795b' then 'RapidPlan Training'
when '89294981-6053-4308-8d75-add1c2d2b1df' then 'RapidPath'
when 'e9d3b428-6582-45fe-96bc-9b52e6ea4578' then 'RapidPath Training'
when 'bfbfb16b-bc0b-4993-a221-c5e3a87410b6' then 'RapidPlan Online'
when '92a5e8e7-ece4-43c4-8ce4-526fbb7272e0' then 'RapidPath Online'
else 'Addon' end product,

orderdate standardstartdate,
date(CONVERT_TIMEZONE('UTC', a.periodend)) standardenddate

from invarion_prod.staging.classified_transactions a
left join invarion_prod.staging.company b on b.id = a.currentbillingentityid

where a.transactionarr > 0 
and nvl(lower(a.type),'-') not in ('refund') 
and a.matchingrefund not in (select distinct id from invarion_prod.staging.classified_transactions where nvl(lower(a.type),'-') in ('refund') )
and product = :tmp_product
order by a.currentbillingentityid, a.id, orderdate
);

end if;

create or replace transient table invarion_prod.staging.invarion_adjusted_end_date_pg as(
select a.customer_id, a.ordernumber, a.orderdate, a.standardstartdate, a.standardenddate,a.subtypeatordertime,
greatest(a.standardenddate,least(a.standardenddate+:grace_period,  case when min(b.standardstartdate) is null then a.standardenddate+:grace_period
else min(b.standardstartdate) end)) standardenddate_adjusted

from invarion_prod.staging.invarion_orders_stg_pg a
left join invarion_prod.staging.invarion_orders_stg_pg b on a.customer_id = b.customer_id
and b.standardstartdate > a.standardstartdate

group by 1,2,3,4,5,6
order by 1,2,3,4
);

alter table invarion_prod.staging.invarion_orders_stg_pg add column standardenddate_adjusted date default null;
update invarion_prod.staging.invarion_orders_stg_pg t1
  set t1.standardenddate_adjusted = t2.standardenddate_adjusted
from invarion_prod.staging.invarion_adjusted_end_date_pg t2
where t1.customer_id = t2.customer_id and t1.ordernumber = t2.ordernumber;

drop table invarion_prod.staging.invarion_adjusted_end_date_pg;

return 1;

end;