------------------------------Creating the staging table-----------------------------
create or replace transient table invarion_prod.staging.invarion_orders_stg as
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
case 
when upper(a.termlength) = 'YEARLY' then dateadd('year',1,orderdate)
when upper(a.termlength) = 'MONTHLY' then dateadd('month',1,orderdate)
when upper(a.termlength) = 'THREE YEAR' then dateadd('month',36,orderdate)
end standardenddate

from invarion_prod.staging.classified_transactions a
left join invarion_prod.staging.company b on b.id = a.currentbillingentityid

where a.transactionarr > 0 
and nvl(lower(a.type),'-') not in ('refund') 
and a.matchingrefund not in (select distinct id from invarion_prod.staging.classified_transactions where nvl(lower(a.type),'-') in ('refund') )
order by a.currentbillingentityid, a.id, orderdate
);

set grace_period=0;

create or replace transient table invarion_prod.staging.invarion_adjusted_end_date as(
select a.customer_id, a.ordernumber, a.orderdate, a.standardstartdate, a.standardenddate,a.subtypeatordertime,
greatest(a.standardenddate,least(a.standardenddate+$grace_period,  case when min(b.standardstartdate) is null then a.standardenddate+$grace_period
else min(b.standardstartdate) end)) standardenddate_adjusted

from invarion_prod.staging.invarion_orders_stg a
left join invarion_prod.staging.invarion_orders_stg b on a.customer_id = b.customer_id
and b.standardstartdate > a.standardstartdate

group by 1,2,3,4,5,6
order by 1,2,3,4
);

alter table invarion_prod.staging.invarion_orders_stg add column standardenddate_adjusted date default null;
update invarion_prod.staging.invarion_orders_stg t1
  set t1.standardenddate_adjusted = t2.standardenddate_adjusted
from invarion_prod.staging.invarion_adjusted_end_date t2
where t1.customer_id = t2.customer_id and t1.ordernumber = t2.ordernumber;

drop table invarion_prod.staging.invarion_adjusted_end_date;

---------------------------------------Create refund table
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
case 
when upper(a.termlength) = 'YEARLY' then dateadd('year',1,orderdate)
when upper(a.termlength) = 'MONTHLY' then dateadd('month',1,orderdate)
when upper(a.termlength) = 'THREE YEAR' then dateadd('month',36,orderdate)
end standardenddate

from invarion_prod.staging.classified_transactions a
left join invarion_prod.staging.company b on b.id = a.currentbillingentityid

where a.transactionarr > 0 
and a.matchingrefund in (select distinct id from invarion_prod.staging.classified_transactions where nvl(lower(type),'-') in ('refund') )
order by a.currentbillingentityid, a.id, orderdate
);