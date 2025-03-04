create or replace procedure invarion_prod.staging.invarion_sp01_load_staging(sp_product string default 'all')
returns number
language sql
as

declare
tmp_product string := null;

begin

let grace_period integer := 0;
tmp_product := sp_product;

----------------------------Flag legacy customers
create or replace transient table invarion_prod.prod.invarion_legacy_customers as
(
select res.currentbillingentityid legacy_customer_id, date(res.periodend) legacy_last_transaction_date 
from
(
select currentbillingentityid, periodstart, periodend, legacy_transaction,
row_number() over (partition by currentbillingentityid order by periodstart desc) as r_n
FROM invarion_prod.STAGING.TRANSACTIONS_RAW
where type != 'Refund'
order by 1, 2 desc
) res
where res.r_n=1 and res.legacy_transaction=true
);

-----------------------Pick refund transactions that are mapped to a charge
create or replace transient table invarion_prod.staging.refunded_transactions_mapping as
(
select refund.id refund_id, 
concat(refund.currentbillingentityid, 
'|', refund.periodstart, '|', 
refund.periodend, '|', 
refund.originalamount
,'|', refund.objectid
) key_col,
charge.id charge_id, refund.originalamount

from invarion_prod.staging.transactions_raw refund
left join(
select id, concat(currentbillingentityid, '|', periodstart, '|', periodend, '|', originalamount
,'|', objectid
) key_col, connectedToType
from invarion_prod.staging.transactions_raw
where lower(type)!='refund' and originalamount>0
) charge on charge.key_col = 
concat(refund.currentbillingentityid, '|', 
refund.periodstart, '|', 
refund.periodend, '|', 
refund.originalamount
, '|', refund.objectid
)
where lower(refund.type)='refund' and charge.id is not null 
and refund.originalamount>0
)
;

--Identify duplicate refunds against a single charge or vice versa
create or replace transient table invarion_prod.staging.dup_refunds as
(select refund_id, count(distinct charge_id) n 
from invarion_prod.staging.refunded_transactions_mapping
group by 1
having n>1
);
create or replace transient table  invarion_prod.staging.dup_charges as
(select charge_id , count(distinct refund_id) n 
from invarion_prod.staging.refunded_transactions_mapping
group by 1
having n>1
);


-----------------------Pick refund transactions that are not mapped to a charge
create or replace transient table invarion_prod.staging.not_refunded_transactions as
(
select refund.id refund_id, 
concat(refund.currentbillingentityid, 
'|', refund.periodstart, '|', 
refund.periodend, '|', 
refund.originalamount
,'|', refund.objectid
) key_col,
charge.id charge_id, refund.originalamount

from invarion_prod.staging.transactions_raw refund
left join(
select id, concat(currentbillingentityid, '|', periodstart, '|', periodend, '|', originalamount
,'|', objectid
) key_col, connectedToType
from invarion_prod.staging.transactions_raw
where lower(type)!='refund' and originalamount>0
) charge on charge.key_col = 
concat(refund.currentbillingentityid, '|', 
refund.periodstart, '|', 
refund.periodend, '|', 
refund.originalamount
, '|', refund.objectid
)
where lower(refund.type)='refund' and charge.id is null 
and refund.originalamount>0

union

select refund.id refund_id, 
concat(refund.currentbillingentityid, 
'|', refund.periodstart, '|', 
refund.periodend, '|', 
refund.originalamount
,'|', refund.objectid
) key_col,
charge.id charge_id, refund.originalamount

from invarion_prod.staging.transactions_raw refund
left join(
select id, concat(currentbillingentityid, '|', periodstart, '|', periodend, '|', originalamount
,'|', objectid
) key_col, connectedToType
from invarion_prod.staging.transactions_raw
where lower(type)!='refund' and originalamount>0
) charge on charge.key_col = 
concat(refund.currentbillingentityid, '|', 
refund.periodstart, '|', 
refund.periodend, '|', 
refund.originalamount
, '|', refund.objectid
)
where refund.id in (select distinct refund_id from invarion_prod.staging.dup_refunds)
or refund.id in (select distinct refund_id from invarion_prod.staging.refunded_transactions_mapping where charge_id in
(select distinct charge_id from invarion_prod.staging.dup_charges))
)
;

---delete duplicates from the mapping table
delete from invarion_prod.staging.refunded_transactions_mapping where refund_id in (select distinct refund_id from invarion_prod.staging.dup_refunds);
delete from invarion_prod.staging.refunded_transactions_mapping where charge_id in (select distinct charge_id from invarion_prod.staging.dup_charges);


if (sp_product = 'all') then

create or replace transient table invarion_prod.staging.invarion_orders_stg_pg as
(select a.currentbillingentityid customer_id, a.id ordernumber, date(a."DATE") orderdate, type,
case when day(a.periodstart) = day(periodend) then 
    (12*(a.originalamount*case upper(trim(originalcurrencycode))
    when 'USD' then 1
    when 'AUD' then 0.66
    when 'NZD' then 0.584779
    when 'CAD' then 0.730438
    when 'GBP' then 1.22451
    when 'EUR' then 1.06670
    else 1
    end
    ))/(case when datediff('months',a.periodstart, a.periodend)<=0 then 1 else datediff('months',a.periodstart, a.periodend) end)
when day(a.periodstart) != day(periodend) and a.periodend>=a.periodstart then 
    (365*(a.originalamount*case upper(trim(originalcurrencycode))
    when 'USD' then 1
    when 'AUD' then 0.66
    when 'NZD' then 0.584779
    when 'CAD' then 0.730438
    when 'GBP' then 1.22451
    when 'EUR' then 1.06670
    else 1
    end
    ))/truncate(TIMESTAMPDIFF('hours',a.periodstart, a.periodend)/24)
else 0
end totalordercost, 
case when datediff('days', a.periodstart, a.periodend) between 0 and 31 then 'Monthly'
when datediff('days', a.periodstart, a.periodend) between 32 and 370 then 'Yearly'
when datediff('days', a.periodstart, a.periodend) > 370 then 'Three Year'
end subtypeatordertime,
upper(trim(b.name)) company,
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
originalcurrencycode,

date(a.periodstart) standardstartdate,
date(a.periodend) standardenddate

from invarion_prod.staging.transactions_raw a
left join invarion_prod.staging.company_raw b on b.id = a.currentbillingentityid

--where totalordercost > 0 
--and nvl(lower(a.type),'-') not in ('refund') 
--and a.id not in (select distinct charge_id from invarion_prod.staging.refunded_transactions_mapping)
order by a.currentbillingentityid, a.id, orderdate
);

---------------Load refund tables
create or replace transient table invarion_prod.staging.invarion_refunded_orders_stg as
(select * 
from invarion_prod.staging.invarion_orders_stg_pg
where totalordercost > 0 
and ordernumber in (select distinct charge_id from invarion_prod.staging.refunded_transactions_mapping)
order by customer_id, ordernumber, orderdate
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
where totalordercost > 0 
and nvl(lower(type),'-') not in ('refund') 
and ordernumber not in (select distinct charge_id from invarion_prod.staging.refunded_transactions_mapping)
group by 1) t2
where t1.customer_id = t2.customer_id;

alter table invarion_prod.prod.invarion_refunded_arr_table_adj add column country_name string;
update invarion_prod.prod.invarion_refunded_arr_table_adj t1
set t1.country_name = t2.country
from whsoftware_prod.staging.whs_region_dim_unified t2
where upper(trim(t1.countrycode)) = upper(trim(t2.country_code));

------------------------------------Load Not refunded transactions
create or replace transient table invarion_prod.staging.invarion_not_refunded_orders_stg as
(select *
from invarion_prod.staging.invarion_orders_stg_pg
where totalordercost > 0 
and ordernumber in (select distinct refund_id from invarion_prod.staging.not_refunded_transactions)
order by customer_id, ordernumber, orderdate
);

create or replace transient table invarion_prod.prod.invarion_not_refunded_arr_table_adj as
(
select c.pivot_month, a.*,
case when c.pivot_month < last_day(a.standardenddate) then -1*a.totalordercost else 0 end not_refunded_arr_currency,
'Not Refunded' customer_classification

from invarion_prod.staging.invarion_not_refunded_orders_stg a

left join (select distinct(last_day(ds)) pivot_month from whsoftware_prod.staging.whs_date_dim order by 1) c 
on c.pivot_month >= last_day(a.standardstartdate) and c.pivot_month < last_day(a.standardenddate)

where c.pivot_month is not null

);

alter table invarion_prod.prod.invarion_not_refunded_arr_table_adj add column first_order_date date;
update invarion_prod.prod.invarion_not_refunded_arr_table_adj t1
set t1.first_order_date = t2.first_order_date
from (select customer_id, min(orderdate) first_order_date 
from invarion_prod.staging.invarion_orders_stg_pg
where totalordercost > 0 
and nvl(lower(type),'-') not in ('refund') 
and ordernumber not in (select distinct charge_id from invarion_prod.staging.refunded_transactions_mapping)
group by 1) t2
where t1.customer_id = t2.customer_id;

alter table invarion_prod.prod.invarion_not_refunded_arr_table_adj add column country_name string;
update invarion_prod.prod.invarion_not_refunded_arr_table_adj t1
set t1.country_name = t2.country
from whsoftware_prod.staging.whs_region_dim_unified t2
where upper(trim(t1.countrycode)) = upper(trim(t2.country_code));

------------------------------------Load Trial transactions
create or replace transient table invarion_prod.staging.invarion_trial_orders_stg as
(select *
from invarion_prod.staging.invarion_orders_stg_pg
where totalordercost = 0 and lower(type)!='refund'
and ordernumber not in (select distinct charge_id from invarion_prod.staging.refunded_transactions_mapping)
order by customer_id, ordernumber, orderdate
);

create or replace transient table invarion_prod.prod.invarion_trial_arr_table_adj as
(
select c.pivot_month, a.*,
0 trial_arr_currency,
'Trial' customer_classification

from invarion_prod.staging.invarion_trial_orders_stg a

left join (select distinct(last_day(ds)) pivot_month from whsoftware_prod.staging.whs_date_dim order by 1) c 
on c.pivot_month = last_day(a.standardstartdate)

where c.pivot_month is not null
);

alter table invarion_prod.prod.invarion_trial_arr_table_adj add column country_name string;
update invarion_prod.prod.invarion_trial_arr_table_adj t1
set t1.country_name = t2.country
from whsoftware_prod.staging.whs_region_dim_unified t2
where upper(trim(t1.countrycode)) = upper(trim(t2.country_code));
-------------------------------------------------------
else 

create or replace transient table invarion_prod.staging.invarion_orders_stg_pg as
(select a.currentbillingentityid customer_id, a.id ordernumber, date(a."DATE") orderdate, type,
case when day(a.periodstart) = day(periodend) then 
    (12*(a.originalamount*case upper(trim(originalcurrencycode))
    when 'USD' then 1
    when 'AUD' then 0.66
    when 'NZD' then 0.584779
    when 'CAD' then 0.730438
    when 'GBP' then 1.22451
    when 'EUR' then 1.06670
    else 1
    end
    ))/(case when datediff('months',a.periodstart, a.periodend)<=0 then 1 else datediff('months',a.periodstart, a.periodend) end)
when day(a.periodstart) != day(periodend) and a.periodend>=a.periodstart then 
    (365*(a.originalamount*case upper(trim(originalcurrencycode))
    when 'USD' then 1
    when 'AUD' then 0.66
    when 'NZD' then 0.584779
    when 'CAD' then 0.730438
    when 'GBP' then 1.22451
    when 'EUR' then 1.06670
    else 1
    end
    ))/truncate(TIMESTAMPDIFF('hours',a.periodstart, a.periodend)/24)
else 0
end totalordercost, 
case when datediff('days', a.periodstart, a.periodend) between 0 and 31 then 'Monthly'
when datediff('days', a.periodstart, a.periodend) between 32 and 370 then 'Yearly'
when datediff('days', a.periodstart, a.periodend) > 370 then 'Three Year'
end subtypeatordertime,
upper(trim(b.name)) company,
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
originalcurrencycode,

date(a.periodstart) standardstartdate,
date(a.periodend) standardenddate

from invarion_prod.staging.transactions_raw a
left join invarion_prod.staging.company_raw b on b.id = a.currentbillingentityid

where totalordercost > 0 
and nvl(lower(a.type),'-') not in ('refund') 
and a.id not in (select distinct charge_id from invarion_prod.staging.refunded_transactions_mapping)
and product = :tmp_product
order by a.currentbillingentityid, a.id, orderdate
);

end if;

delete from invarion_prod.staging.invarion_orders_stg_pg 
where totalordercost = 0 
or nvl(lower(type),'-') in ('refund') 
or ordernumber in (select distinct charge_id from invarion_prod.staging.refunded_transactions_mapping);

create or replace transient table invarion_prod.staging.invarion_adjusted_end_date_pg as(
select a.customer_id, a.ordernumber, a.orderdate, a.standardstartdate, a.standardenddate,a.subtypeatordertime,
greatest(a.standardenddate,least(a.standardenddate+:grace_period,  case when min(b.standardstartdate) is null then a.standardenddate+:grace_period
else min(b.standardstartdate) end)) standardenddate_adjusted

from invarion_prod.staging.invarion_orders_stg_pg a
left join invarion_prod.staging.invarion_orders_stg_pg b on a.customer_id = b.customer_id
and b.standardstartdate > a.standardstartdate

where a.totalordercost > 0 
and nvl(lower(a.type),'-') not in ('refund') 
and a.ordernumber not in (select distinct charge_id from invarion_prod.staging.refunded_transactions_mapping)

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