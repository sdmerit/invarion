create or replace transient table invarion_prod.staging.invarion_registration_dates_adj as(
select kw_number clientid, company, min(standardstartdate) client_start_date, max(standardenddate_adjusted) client_end_date
from invarion_prod.staging.invarion_orders_stg
group by 1,2
);

-----------------------Enercalc rollforward adjusted
create or replace transient table invarion_prod.prod.invarion_arr_rollforward_adj as(
select 'Invarion Adjusted' source, b.clientid, 
a.pivot_month, upper(trim(b.company)) company, b.client_end_date,

sum(c.arr_currency*1) arr_spot_rate_usd, 
lag(arr_spot_rate_usd, 1 ) respect nulls
    over ( partition by source, b.clientid order by a.pivot_month asc) arr_spot_rate_usd_prev,
nvl(arr_spot_rate_usd,0) - nvl(arr_spot_rate_usd_prev,0) arr_spot_rate_usd_change,
    
sum(c.arr_currency*1) arr_const_curr_py_usd,
lag(arr_const_curr_py_usd, 1 ) respect nulls
    over ( partition by source, b.clientid order by a.pivot_month asc) arr_const_curr_py_usd_prev,
nvl(round(arr_const_curr_py_usd,5),0) - nvl(round(arr_const_curr_py_usd_prev,5),0) arr_const_curr_py_usd_change,
--change required for reactivations
case when (arr_const_curr_py_usd_prev is null or arr_const_curr_py_usd_prev=0) and arr_const_curr_py_usd>0 then 'New         '

when a.pivot_month = last_day(case when b.client_end_date = last_day(b.client_end_date) then b.client_end_date+1 else b.client_end_date end) 
and arr_const_curr_py_usd_change<0 then 'Churn'

when arr_const_curr_py_usd_prev>0 and (arr_const_curr_py_usd=0 or arr_const_curr_py_usd is null) then 'Churn'

when arr_const_curr_py_usd_change=0 and arr_const_curr_py_usd>0 then 'ForEx'
when nvl(arr_const_curr_py_usd_prev,0)=0 and nvl(arr_const_curr_py_usd,0)=0 then '-'

when arr_const_curr_py_usd_change < 0 then 'Downgrade'
when arr_const_curr_py_usd_change > 0 then 'Upgrade'
else 'Unknown' end customer_classification

from (select distinct last_day(ds) pivot_month from whsoftware_prod.staging.whs_date_dim order by 1) a

left join invarion_prod.staging.invarion_registration_dates_adj b on a.pivot_month between last_day(b.client_start_date) 
and last_day(case when b.client_end_date = last_day(b.client_end_date) then b.client_end_date+1 else b.client_end_date end)

left join invarion_prod.prod.invarion_arr_table_adj c on c.kw_number = b.clientid and c.company = b.company and c.pivot_month = a.pivot_month

group by 1,2,3,4,5
order by 1,2,3
);

------------------Create a reference for customer create date
create or replace transient table invarion_prod.staging.invarion_cust_createdate as
(
    select currentbillingentityid kw_number, date(min("DATE")) datecustomer
    from invarion_prod.staging.classified_transactions
    group by 1
);

update invarion_prod.prod.invarion_arr_rollforward_adj t1
set t1.customer_classification = 'Reactivation'
from invarion_prod.staging.invarion_cust_createdate t2
where t1.clientid = t2.kw_number
and trim(t1.customer_classification) = 'New' and t1.pivot_month > last_day(t2.datecustomer);

drop table invarion_prod.staging.invarion_cust_createdate;

update invarion_prod.prod.invarion_arr_rollforward_adj t1
set t1.customer_classification = trim(t1.customer_classification);

--------------------Attach customer classification to ARR table and update arr and churn_arr_currency

alter table invarion_prod.prod.invarion_arr_table_adj add column customer_classification string default null;
update invarion_prod.prod.invarion_arr_table_adj t1
set t1.customer_classification=trim(t2.customer_classification)
from  invarion_prod.prod.invarion_arr_rollforward_adj t2
where t1.kw_number = t2.clientid and t1.pivot_month = t2.pivot_month;

update invarion_prod.prod.invarion_arr_table_adj t1
set t1.arr_currency=0
where t1.customer_classification = 'Churn';

update invarion_prod.prod.invarion_arr_table_adj t1
set t1.churn_arr_currency=0
where t1.customer_classification != 'Churn';

-------------------------------------------Append churn data to ARR adjusted table

create or replace transient table invarion_prod.prod.invarion_arr_table_adj_final as
(select * 
from invarion_prod.prod.invarion_arr_table_adj

union all

select last_day(res.pivot_month+1) pivot_month, res.kw_number, res.ordernumber, res.orderdate, res.totalordercost, res.subtypeatordertime,
res.company, res.company_type, res.countrycode, res.state, res.city, res.product, res.standardstartdate,
res.standardenddate, res.standardenddate_adjusted, 0 arr_currency,
-1*res.arr_cum churn_arr_currency,
'Churn' customer_classification
from
(
select *,
lag(pivot_month, -1 ) respect nulls
    over ( partition by kw_number order by pivot_month asc, arr_currency desc) next_pivot_month,
datediff('month',pivot_month,next_pivot_month) diff,
case when diff>1 or diff is null then 'needs churn' else '-' end category,
sum(arr_currency) over (partition by kw_number, pivot_month) arr_cum
from invarion_prod.prod.invarion_arr_table_adj
where arr_currency>0
order by kw_number, pivot_month asc
) res
where res.category = 'needs churn'
);

--------------------------------Append upgrade and downgrade details to ARR adjusted table
create or replace transient table invarion_prod.staging.invarion_upg_dwg_ref_adj as
(select *,
lag(arr_cum, 1,0 ) respect nulls 
    over(partition by kw_number order by pivot_month asc) prev_arr_cum,
case when customer_classification = 'Upgrade' then arr_cum-prev_arr_cum else 0 end upgrade_curr,
case when customer_classification = 'Downgrade' then arr_cum-prev_arr_cum else 0 end downgrade_curr
from(
select kw_number, pivot_month, ordernumber, arr_currency, churn_arr_currency, 
sum(arr_currency) over (partition by kw_number, pivot_month) arr_cum,
customer_classification 
from invarion_prod.prod.invarion_arr_table_adj_final
order by kw_number,pivot_month asc
)
order by kw_number,pivot_month, arr_currency asc
);

alter table invarion_prod.prod.invarion_arr_table_adj_final add column upgrade_currency float, downgrade_currency float, new_currency float, reactivation_currency float;
update invarion_prod.prod.invarion_arr_table_adj_final t1
set t1.upgrade_currency = t2.upgrade_curr, t1.downgrade_currency = t2.downgrade_curr,
t1.new_currency = (case when t1.customer_classification = 'New' then t1.arr_currency else 0 end),
t1.reactivation_currency = (case when t1.customer_classification = 'Reactivation' then t1.arr_currency else 0 end)
from invarion_prod.staging.invarion_upg_dwg_ref_adj t2
where t1.ordernumber = t2.ordernumber and t1.kw_number = t2.kw_number and t1.pivot_month = t2.pivot_month; 

-----------------------------------Adding first order date
alter table invarion_prod.prod.invarion_arr_table_adj_final add column first_order_date date;
update invarion_prod.prod.invarion_arr_table_adj_final t1
set t1.first_order_date = t2.first_order_date
from (select kw_number, min(orderdate) first_order_date 
from invarion_prod.staging.invarion_orders_stg
group by 1) t2
where t1.kw_number = t2.kw_number;


drop table if exists invarion_prod.prod.invarion_arr_table_adj;
drop table if exists invarion_prod.staging.invarion_upg_dwg_ref_adj;