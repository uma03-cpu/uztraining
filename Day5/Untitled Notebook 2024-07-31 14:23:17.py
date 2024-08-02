# Databricks notebook source
# MAGIC %run "/Workspace/Users/uma.zanwar@outlook.com/Databricks training/Hexware Training/Day5/Includes"

# COMMAND ----------

dbutils.widgets.text("environment"," ")
w_text = dbutils.widgets.get("environment")

# COMMAND ----------

w_text
