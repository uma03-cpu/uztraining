# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog uzdatabricks;
# MAGIC create schema if not exists silver;
# MAGIC create schema if not exists gold

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table uzdatabricks.silver.richest_silver as
# MAGIC select name,country,industry,Net_Worth_in_billions from uzdatabricks.bronze.richest_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table uzdatabricks.gold.country_industry_count
# MAGIC select country, industry, count(1) count from uzdatabricks.silver.richest_silver
# MAGIC group by country, industry
# MAGIC order by 1,3 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table uzdatabricks.gold.name_net_worth
# MAGIC select  name, count(1) count  from uzdatabricks.silver.richest_silver
# MAGIC group by name order by 2 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace gold.name_net_worth as
# MAGIC select name,sum(Net_Worth_in_billions) from silver.richest_silver
# MAGIC group by name

# COMMAND ----------


