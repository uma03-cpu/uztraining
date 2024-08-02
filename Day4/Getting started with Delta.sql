-- Databricks notebook source
-- MAGIC %python
-- MAGIC Data Object in the lakehouse
-- MAGIC
-- MAGIC Catalog
-- MAGIC Schema
-- MAGIC Table, view, Functions
-- MAGIC
-- MAGIC Tables: Managed Table and External Table
-- MAGIC Views: Std , temp, global temp

-- COMMAND ----------

Way to create a delta table
1. SQL
2. Python
3. DataFrame

-- COMMAND ----------

create schema datamaster.hexaware;

-- COMMAND ----------

create table datamaster.hexaware.demo(id int, name string)

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/mnt/datamasterdatabricks/raw/

-- COMMAND ----------

create table datamaster.hexaware.demo2(id int, name string, age int) LOCATION 'abfss://raw@datamasterdatabricks.dfs.core.windows.net/delta/demo2'

-- COMMAND ----------

insert into datamaster.hexaware.demo2 values(111,'naval', 32)

-- COMMAND ----------

Insert into datamaster.hexaware.demo values(1,'naval')

-- COMMAND ----------

drop table datamaster.hexaware.demo2

-- COMMAND ----------

select * from delta.`abfss://raw@datamasterdatabricks.dfs.core.windows.net/delta/demo2`

-- COMMAND ----------

select * from datamaster.hexaware.demo

-- COMMAND ----------

describe extended datamaster.hexaware.demo

-- COMMAND ----------


