-- Big project for SQL
-- Link instruction: https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0


-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
SELECT FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d',date)) as month
  ,sum(totals.visits) as visits
  ,sum(totals.pageviews) as pageviews
  ,sum(totals.transactions) as transactions
  ,(sum(totals.totalTransactionRevenue)/1000000) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE _table_suffix between '20170101' and '20170331'
  GROUP BY month
  ORDER BY month

-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
SELECT trafficsource.source as source
  ,sum(totals.visits) as total_visit
  ,sum(totals.bounces) as total_no_of_bounces
  ,(Safe_divide(sum(totals.bounces),sum(totals.visits))*100) as bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE _table_suffix between '20170701' and '20170731'
  GROUP BY source
  ORDER BY total_visit DESC

-- Query 3: Revenue by traffic source by week, by month in June 2017
#standardSQL
SELECT 
  concat('Month')as time_type
  ,FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d',date)) as time
  ,trafficsource.source as source
  ,(sum(totals.totalTransactionRevenue)/1000000) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE _table_suffix between '20170601' and '20170630'
  GROUP BY source,time
UNION ALL
SELECT 
  concat('Week')as time_type
  ,FORMAT_DATE('%Y%W',PARSE_DATE('%Y%m%d',date)) as time
  ,trafficsource.source as source
  ,(sum(totals.totalTransactionRevenue)/1000000) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE _table_suffix between '20170601' and '20170630'
  GROUP BY source,time
  ORDER BY revenue DESC

--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
SELECT 
  FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d',date)) as month
  ,sum(case when totals.transactions >=1 THEN totals.pageviews END)/count(Distinct case when totals.transactions >=1 THEN fullVisitorId END) as avg_non_pageviews_purchase
  ,sum(case when totals.transactions is null THEN totals.pageviews END)/count(Distinct case when totals.transactions is null THEN fullVisitorId END) as avg_pageviews_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE _table_suffix between '20170601' and '20170731'
  GROUP BY month

-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
SELECT 
  FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d',date)) as month
  ,sum(case when totals.transactions >=1 THEN totals.transactions END)/count(Distinct case when totals.transactions >=1 THEN fullVisitorId END) as avg_non_pageviews_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE _table_suffix between '20170701' and '20170731'
  GROUP BY month

-- Query 06: Average amount of money spent per session
#standardSQL
SELECT 
  FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d',date)) as month
 ,Round(sum(totals.totalTransactionRevenue)/count(totals.transactions),2) as avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE _table_suffix between '20170701' and '20170731'
  AND totals.transactions IS NOT NULL
  GROUP BY month

-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
#standardSQL
SELECT 
  product.v2ProductName as other_purchased_products
  ,sum(product.productQuantity) as quantity
FROM 
  `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
  ,UNNEST(hits) AS hits
  ,UNNEST(hits.product) as product
    WHERE product.productRevenue is not null
    AND fullVisitorId IN 
      (SELECT distinct fullVisitorId
      FROM 
        `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
        ,UNNEST(hits) AS hits
        ,UNNEST(hits.product) as product
        WHERE product.v2ProductName = "YouTube Men's Vintage Henley"
        AND product.productRevenue is not null)
    AND product.v2ProductName != "YouTube Men's Vintage Henley"
  GROUP BY other_purchased_products
  ORDER BY quantity DESC

--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL

SELECT
  FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d',date)) as month
  ,count(case when hits.eCommerceAction.action_type = '2' then 1 END) as num_product_view
  ,count(case when hits.eCommerceAction.action_type = '3' then 1 END) as num_addtocart
  ,count(case when hits.eCommerceAction.action_type = '6' then 1 END) as num_purchase
  ,round((count(case when hits.eCommerceAction.action_type = '3' then 1 END)/count(case when hits.eCommerceAction.action_type = '2' then 1 END)*100),2) as add_to_cart_rate
  ,round((count(case when hits.eCommerceAction.action_type = '6' then 1 END)/count(case when hits.eCommerceAction.action_type = '3' then 1 END)*100),2) as purchase_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  ,UNNEST(hits) AS hits
  ,UNNEST(hits.product) as product
  WHERE _table_suffix between '20170101' and '20170331'
  GROUP BY month
  ORDER BY month