# Databricks notebook source
# spark.conf.set("spark.sql.shuffle.partitions",5)

# COMMAND ----------

customer_data = [(1001, 'Alice', 'Johnson', '2024-01-15'),
(1002, 'Bob', 'Smith', '2024-01-18'),
(1003, 'Carol', 'Davis', '2024-01-22'),
(1004, 'David', 'Miller', '2024-01-25'),
(1005, 'Emily', 'Martinez', '2024-01-28'),
(1006, 'Frank', 'Taylor', '2024-01-30'),
(1007, 'Grace', 'Anderson', '2024-02-02'),
(1008, 'Harry', 'White', '2024-02-05'),
(1009, 'Iris', 'Brown', '2024-02-08'),
(1010, 'Jack', 'Wilson', '2024-02-12')]

# COMMAND ----------

customer_schema = ['Customer_id','First_name','Last_name','Order_date']

# COMMAND ----------

customer_df = spark.createDataFrame(data=customer_data,schema=customer_schema)

# COMMAND ----------

customer_df.display()

# COMMAND ----------

sales_data = [
    (1, 1001, 'ProductA', 2, 50.0, '2024-01-15'),
    (2, 1002, 'ProductB', 1, 75.0, '2024-01-18'),
    (3, 1003, 'ProductC', 3, 30.0, '2024-01-22'),
    (4, 1004, 'ProductA', 1, 50.0, '2024-01-25'),
    (5, 1005, 'ProductB', 2, 75.0, '2024-01-28'),
    (6, 1006, 'ProductC', 1, 30.0, '2024-01-30'),
    (7, 1007, 'ProductA', 2, 50.0, '2024-02-02'),
    (8, 1008, 'ProductB', 1, 75.0, '2024-02-05'),
    (9, 1009, 'ProductC', 3, 30.0, '2024-02-08'),
    (10, 1010, 'ProductA', 1, 50.0, '2024-02-12'),
    # Adding some repeated entries for demonstration
    (11, 1002, 'ProductB', 1, 75.0, '2024-01-18'),  # Customer 1002 with ProductB repeated
    (12, 1006, 'ProductC', 1, 30.0, '2024-01-30')]

# COMMAND ----------

sales_schema = ['OrderID','CustomerID','Product','Quantity','Price','OrderDate']

# COMMAND ----------

sales_df = spark.createDataFrame(data=sales_data,schema=sales_schema)

# COMMAND ----------

sales_df.display()

# COMMAND ----------

sort_merge_df = customer_df.join(sales_df,customer_df["Customer_id"]==sales_df["CustomerID"], "inner")

# COMMAND ----------

sort_merge_df.display()

# COMMAND ----------

sort_merge_df.explain()

# COMMAND ----------

from pyspark.sql.functions import broadcast

# COMMAND ----------

df=customer_df.join(broadcast(sales_df),customer_df["Customer_id"]==sales_df["CustomerID"], "inner")

# COMMAND ----------

df.explain()

# COMMAND ----------

df.display()

# COMMAND ----------

leftjoin_df = customer_df.join(sales_df,customer_df["Customer_id"]==sales_df["CustomerID"], "left")

# COMMAND ----------

leftjoin_df.display()

# COMMAND ----------


