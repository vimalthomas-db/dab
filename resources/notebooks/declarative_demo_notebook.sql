-- Databricks notebook source
create or refresh streaming table vjs.demo_db.streaming_sample
as
select * from stream(samples.tpch.orders)

-- COMMAND ----------

--create aggregated view vjs.demo_db.streaming_sample_agg


create or replace materialized view agg_orders
as
select 
count(o_orderkey) as total_orders,
avg(o_totalprice) as avg_price,
sum(o_totalprice) as total_revenue
from vjs.demo_db.streaming_sample
group by o_orderstatus
