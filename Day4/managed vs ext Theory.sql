-- Databricks notebook source
create table tbalaname()

-- COMMAND ----------

df.write.saveAsTable("tablename")

-- COMMAND ----------

create table tblname () location 'mnt/adls'

-- COMMAND ----------

df.write.option("path","/mnt/adls").saveAsTable("tablename")
