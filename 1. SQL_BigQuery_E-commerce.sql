-- Big project for SQL
-- Link instruction: https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0

-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
SELECT 
  format_date('%Y%m',parse_date("%Y%m%d",date)) as month,
  sum(totals.visits) as visits,
  sum(totals.pageviews) as pageviews,
  sum(totals.transactions) as transactions,
  sum(totals.totalTransactionRevenue)/power(10,6) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` 
group by month
having month in ('201701','201702','201703')
order by month



-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
with total_data as
  (SELECT 
    trafficsource.source as source,
    sum(totals.visits) as total_visits,
    sum(totals.bounces) as total_no_of_bounces
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
  group by trafficsource.source)
select 
  source, total_visits, total_no_of_bounces, 
  round(total_no_of_bounces/total_visits*100,8) as bounce_rate
from total_data
order by total_visits desc



-- Query 3: Revenue by traffic source by week, by month in June 2017
with month_data as
  (SELECT 
    concat('Month') as time_type,
    format_date('%Y%m',parse_date("%Y%m%d",date)) as time,
    trafficsource.source as source,
    sum(totals.totalTransactionRevenue)/power(10,6) as revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
  group by time_type, time, source),
week_data as
  (SELECT 
    concat('Week') as time_type,
    format_date('%Y%W',parse_date("%Y%m%d",date)) as time,
    trafficsource.source as source,
    sum(totals.totalTransactionRevenue)/power(10,6) as revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
  group by time_type, time, source)
select *
from month_data
union all 
select *
from week_data
order by revenue desc



--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL

/*Calculate avg_pageviews_purchase*/
with pur_data as
  (select 
    format_date('%Y%m',parse_date('%Y%m%d',date)) as month,
    round(sum(totals.pageviews)/count(distinct fullVisitorId),8) as avg_pageviews_purchase
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  where totals.transactions >=1
  group by 1),

/*Calculate avg_pageviews_non_purchase*/
non_pur_data as
  (select 
    format_date('%Y%m',parse_date('%Y%m%d',date)) as month,
    round(sum(totals.pageviews)/count(distinct fullVisitorId),9) as avg_pageviews_non_purchase
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  where totals.transactions is null
  group by 1)

/*join 2 cte*/
select p.*, np.avg_pageviews_non_purchase
from pur_data as p
left join non_pur_data as np on p.month = np.month
where p.month in ('201706','201707')
order by month



-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL

/* Define purchaser type */
with pur_data as
  (SELECT
    format_date('%Y%m',parse_date('%Y%m%d',date)) as Month,
    fullVisitorId, 
    sum(totals.transactions) as trans_per_user,
    (case when sum(totals.transactions) >=1 then 'P'
    else 'NP' end) as purchaser_type
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
  group by fullVisitorId, format_date('%Y%m',parse_date('%Y%m%d',date)))

select 
  Month,
  round(sum(trans_per_user)/count(fullvisitorid),9) as Avg_total_transactions_per_user
from pur_data
group by Month, purchaser_type
having purchaser_type = 'P'



-- Query 06: Average amount of money spent per session
#standardSQL
SELECT
    format_date('%Y%m',parse_date('%Y%m%d',date)) as month,
    FORMAT("%'.2f",sum(totals.totaltransactionRevenue)/sum(totals.visits)) as avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` 
where totals.totaltransactionRevenue is not null
group by 1



-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
#standardSQL
with pur_data as 
  (SELECT fullVisitorId
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`, 
    unnest(hits) as hits,
    unnest(hits.product) as product
  where product.v2ProductName = "YouTube Men's Vintage Henley"
    and product.productRevenue is not null)

select  
  product.v2ProductName as other_purchased_products,
  sum(product.productQuantity) as quantity
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  unnest(hits) as hits,
  unnest(hits.product) as product
where product.productRevenue is not null
  and product.v2ProductName <> "YouTube Men's Vintage Henley"
  and fullVisitorId in (select *
                        from pur_data)
group by product.v2ProductName
order by sum(product.productQuantity) desc



--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL

/*Calculate product_view, addtocart, purchase*/
with view_data as
  (SELECT 
    format_date('%Y%m',parse_date('%Y%m%d',date)) as month,
    count(product.v2productname) as num_product_view
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    unnest(hits) as hits,
    unnest(hits.product) as product
  where hits.eCommerceAction.action_type = '2'
  group by 1),
add_data as
  (SELECT 
    format_date('%Y%m',parse_date('%Y%m%d',date)) as month,
    count(product.v2productname) as num_addtocart
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    unnest(hits) as hits,
    unnest(hits.product) as product
  where hits.eCommerceAction.action_type = '3'
  group by 1),
pur_data as
  (SELECT 
    format_date('%Y%m',parse_date('%Y%m%d',date)) as month,
    count(product.v2productname) as num_purchase
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    unnest(hits) as hits, 
    unnest(hits.product) as product
  where hits.eCommerceAction.action_type = '6'
  group by 1)

/*Calculate rates*/
select 
  v.*, num_addtocart, num_purchase,
  round(num_addtocart/num_product_view*100, 2) as add_to_cart_rate, 
  round(num_purchase/num_product_view*100, 2) as purchase_rate
from view_data as v
left join add_data as a on v.month = a.month
left join pur_data as p on v.month = p.month
where v.month in ('201701','201702','201703')
order by v.month


