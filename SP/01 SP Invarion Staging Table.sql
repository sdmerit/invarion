create or replace procedure invarion_prod.staging.invarion_sp01_load_staging(sp_product string default 'all')
returns number
language sql
as

declare
tmp_product string := null;

begin

let grace_period integer := 0;
tmp_product := sp_product;

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

----------------------------Flag legacy licenses
create or replace transient table invarion_prod.staging.invarion_legacy_tracker as
(
select res.*, 

case 
when res.companyid = '468bc43f-a352-41d2-8b68-4a7e0ab097d8' then 0.45*pricegrid.price_th3 --Transport for NSW
when lower(res.product) = 'rapidplan training' and res.latesttransactioncurrency in ('AUD','USD') 
and res.accesstype ilike 'multi%' then 149
when lower(res.product) = 'rapidplan training' and res.latesttransactioncurrency in ('USD') and res.accesstype ilike 'single%' then 99
else pricegrid.price_th1 end renewal_price_stage1, 
case when res.companyid = '468bc43f-a352-41d2-8b68-4a7e0ab097d8' then 0.70*pricegrid.price_th3 --Transport for NSW
else pricegrid.price_th2 end renewal_price_stage2, 
pricegrid.price_th3 renewal_price_stage3,

case when round(res.LatestTransactionamount_per_yr,0)+5<round(renewal_price_stage1,0) then 'Price Transition-Not Started'
when round(res.LatestTransactionamount_per_yr,0)+5<round(renewal_price_stage2,0) then 'Price Transition-Stage 1 Completed'
when round(res.LatestTransactionamount_per_yr,0)+5<round(renewal_price_stage3,0) then 'Price Transition-Stage 2 Completed'
when round(res.LatestTransactionamount_per_yr,0)+5>=round(renewal_price_stage3,0) then 'Price Transition-Stage 3 Completed'
end renewal_status

from
(select a.objectid licenseid,  b.id companyid, coalesce(country.country,b.countrycode) country, b.name companyname,
coalesce(prod.product, 'Addon') product, 
a.objectaccesstype AccessType,
date(latest_transaction."DATE") LatestTransactionDate,
latest_transaction.originalcurrencycode LatestTransactioncurrency,
latest_transaction.originalamount LatestTransactionamount,
latest_transaction.n_years LatestTransaction_years,
case when latest_transaction.n_years>=1 then latest_transaction.originalamount/latest_transaction.n_years 
else latest_transaction.originalamount end LatestTransactionamount_per_yr,
case when lower(latest_transaction.source)='stripe' then true else false end LatestTransactionOnStripe,
case when legacy_lic.licenseid is not null then 'Legacy' else 'Not Legacy' end legacy_status,
date(min(a."DATE")) first_license_purchase_date,
date(max(a.periodend)) CurrentExpirationDate

from invarion_prod.staging.transactions_raw a

left join invarion_prod.staging.company_raw b on b.id = a.currentbillingentityid

left join (select objectid, source, "DATE", datediff('year',date(periodstart), date(periodend)) n_years, originalamount, originalcurrencycode,
row_number() over (partition by objectid order by "DATE" desc) r_n
from invarion_prod.staging.transactions_raw
) latest_transaction on latest_transaction.objectid = a.objectid and latest_transaction.r_n = 1

left join invarion_prod.staging.legacy_licenses legacy_lic on legacy_lic.licenseid = a.objectid

left join invarion_prod.staging.invarion_product_dim prod on prod.objecttypeid = a.objecttypeid

left join whsoftware_prod.staging.whs_region_dim_unified country on upper(trim(b.countrycode)) = upper(trim(country.country_code))

group by 1,2,3,4,5,6,7,8,9,10,11,12,13
) res

left join invarion_prod.staging.invarion_price_rise_grid pricegrid on upper(trim(res.LatestTransactioncurrency)) = upper(trim(pricegrid.currency_code))
and upper(trim(res.product)) = upper(trim(pricegrid.product)) and upper(trim(res.accesstype)) = upper(trim(pricegrid.objectaccesstype))

where res.product in ('RapidPlan','RapidPath','RapidPlan Training','RapidPath Training')
and res.legacy_status = 'Legacy'
)
;
--------------------------------------create orders staging table
create or replace transient table invarion_prod.staging.invarion_orders_stg_pg as
(select a.currentbillingentityid customer_id, a.id ordernumber, date(a."DATE") orderdate, type,
case when day(a.periodstart) = day(periodend) then 
    (12*(a.originalamount*case upper(trim(originalcurrencycode))
    when 'USD' then 1
    when 'AUD' then 0.68095
    when 'NZD' then 0.63136
    when 'CAD' then 0.7549
    when 'GBP' then 1.2727
    when 'EUR' then 1.10363
    when 'DKK' then 0.14801
    when 'SGD' then 0.75388
    when 'FJD' then 0.45310
    when 'UAH' then 0.02604
    when 'AED' then 0.27220
    when 'SAR' then 0.26120
    when 'ZAR' then 0.05455
    when 'AOA' then 0.00120
    when 'MXN' then 0.05893
    when 'TRY' then 0.03370
    when 'MYR' then 0.21751
    when 'HRK' then 0.14650
    else 1
    end
    ))/(case when datediff('months',a.periodstart, a.periodend)<=0 then 1 else datediff('months',a.periodstart, a.periodend) end)
when day(a.periodstart) != day(periodend) and a.periodend>=a.periodstart then 
    (365*(a.originalamount*case upper(trim(originalcurrencycode))
    when 'USD' then 1
    when 'AUD' then 0.68095
    when 'NZD' then 0.63136
    when 'CAD' then 0.7549
    when 'GBP' then 1.2727
    when 'EUR' then 1.10363
    when 'DKK' then 0.14801
    when 'SGD' then 0.75388
    when 'FJD' then 0.45310
    when 'UAH' then 0.02604
    when 'AED' then 0.27220
    when 'SAR' then 0.26120
    when 'ZAR' then 0.05455
    when 'AOA' then 0.00120
    when 'MXN' then 0.05893
    when 'TRY' then 0.03370
    when 'MYR' then 0.21751
    when 'HRK' then 0.14650
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

coalesce(prod.product, 'Addon') product,
originalcurrencycode,

date(a.periodstart) standardstartdate,
date(a.periodend) standardenddate

from invarion_prod.staging.transactions_raw a
left join invarion_prod.staging.company_raw b on b.id = a.currentbillingentityid
left join invarion_prod.staging.invarion_product_dim prod on prod.objecttypeid = a.objecttypeid

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
set t1.country_name = coalesce(t2.country, t1.countrycode)
from whsoftware_prod.staging.whs_region_dim_unified t2
where upper(trim(t1.countrycode)) = upper(trim(t2.country_code));

alter table invarion_prod.prod.invarion_refunded_arr_table_adj add column transaction_source string, connectedto string;
update invarion_prod.prod.invarion_refunded_arr_table_adj t1
set t1.transaction_source = t2.source, t1.connectedto = t2.connectedto
from  invarion_prod.staging.transactions_raw t2
where t1.ordernumber = t2.id;

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
set t1.country_name = coalesce(t2.country, t1.countrycode)
from whsoftware_prod.staging.whs_region_dim_unified t2
where upper(trim(t1.countrycode)) = upper(trim(t2.country_code));

alter table invarion_prod.prod.invarion_not_refunded_arr_table_adj add column transaction_source string, connectedto string;
update invarion_prod.prod.invarion_not_refunded_arr_table_adj t1
set t1.transaction_source = t2.source, t1.connectedto = t2.connectedto
from  invarion_prod.staging.transactions_raw t2
where t1.ordernumber = t2.id;

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
set t1.country_name = coalesce(t2.country, t1.countrycode)
from whsoftware_prod.staging.whs_region_dim_unified t2
where upper(trim(t1.countrycode)) = upper(trim(t2.country_code));

alter table invarion_prod.prod.invarion_trial_arr_table_adj add column transaction_source string, connectedto string;
update invarion_prod.prod.invarion_trial_arr_table_adj t1
set t1.transaction_source = t2.source, t1.connectedto = t2.connectedto
from  invarion_prod.staging.transactions_raw t2
where t1.ordernumber = t2.id;
-------------------------------------------------------
else 

create or replace transient table invarion_prod.staging.invarion_orders_stg_pg as
(select a.currentbillingentityid customer_id, a.id ordernumber, date(a."DATE") orderdate, type,
case when day(a.periodstart) = day(periodend) then 
    (12*(a.originalamount*case upper(trim(originalcurrencycode))
    when 'USD' then 1
    when 'AUD' then 0.68095
    when 'NZD' then 0.63136
    when 'CAD' then 0.7549
    when 'GBP' then 1.2727
    when 'EUR' then 1.10363
    when 'DKK' then 0.14801
    when 'SGD' then 0.75388
    when 'FJD' then 0.45310
    when 'UAH' then 0.02604
    when 'AED' then 0.27220
    when 'SAR' then 0.26120
    when 'ZAR' then 0.05455
    when 'AOA' then 0.00120
    when 'MXN' then 0.05893
    when 'TRY' then 0.03370
    when 'MYR' then 0.21751
    when 'HRK' then 0.14650
    else 1
    end
    ))/(case when datediff('months',a.periodstart, a.periodend)<=0 then 1 else datediff('months',a.periodstart, a.periodend) end)
when day(a.periodstart) != day(periodend) and a.periodend>=a.periodstart then 
    (365*(a.originalamount*case upper(trim(originalcurrencycode))
    when 'USD' then 1
    when 'AUD' then 0.68095
    when 'NZD' then 0.63136
    when 'CAD' then 0.7549
    when 'GBP' then 1.2727
    when 'EUR' then 1.10363
    when 'DKK' then 0.14801
    when 'SGD' then 0.75388
    when 'FJD' then 0.45310
    when 'UAH' then 0.02604
    when 'AED' then 0.27220
    when 'SAR' then 0.26120
    when 'ZAR' then 0.05455
    when 'AOA' then 0.00120
    when 'MXN' then 0.05893
    when 'TRY' then 0.03370
    when 'MYR' then 0.21751
    when 'HRK' then 0.14650
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

coalesce(prod.product, 'Addon') product,
originalcurrencycode,

date(a.periodstart) standardstartdate,
date(a.periodend) standardenddate

from invarion_prod.staging.transactions_raw a
left join invarion_prod.staging.company_raw b on b.id = a.currentbillingentityid
left join invarion_prod.staging.invarion_product_dim prod on prod.objecttypeid = a.objecttypeid

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