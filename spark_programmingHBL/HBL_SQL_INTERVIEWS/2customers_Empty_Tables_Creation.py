# Databricks notebook source
# MAGIC %md
# MAGIC ## Agenda
# MAGIC
# MAGIC
# MAGIC 1 Create Catalog, Database(Schema), Volume
# MAGIC
# MAGIC 2 Create spark session
# MAGIC
# MAGIC 3 create tables
# MAGIC   * customers
# MAGIC   * orders
# MAGIC   * products
# MAGIC   * returns
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1 Create Catalog, DataBase(Schema), Volume
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 2 Create spark session
# MAGIC

# COMMAND ----------


spark.version
# This above is a pre-created spark session. 
# It's used when you want t avoid creating the spark session with teh builder. "spark" is teh precreated session
# We will use this to create a spark session from here onwards

# This belkow is the way to create a spark session using the builder
#spark_session.version  



# COMMAND ----------

pip install duckdb pandas # install duckdb and pandas to be able to query a dataframe


# COMMAND ----------

dbutils.library.restartPython() # tHIS restarts the kernel or python after running the above command to install duckdb

# COMMAND ----------

import pandas as pd
import duckdb
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import to_date, col

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3 Create Tables
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------
# MAGIC ##### CUSTOMERS

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dev.spark_db.customers(
# MAGIC   customerid INT,
# MAGIC 	customer_name STRING,
# MAGIC 	order_id INT,
# MAGIC 	productid INT,
# MAGIC 	product_name STRING,
# MAGIC 	quantity INT,
# MAGIC 	price INT,
# MAGIC 	order_date DATE	
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------
# MAGIC ##### ORDERS

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dev.spark_db.orders(
# MAGIC   order_id INT,
# MAGIC 	customerid INT,
# MAGIC 	productid INT,
# MAGIC 	product_name STRING,
# MAGIC 	region STRING,
# MAGIC 	order_date DATE	
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------
# MAGIC ##### PRODUCTS
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev.spark_db.products(
# MAGIC 	productid INT,
# MAGIC 	product_name STRING,
# MAGIC 	unit_product_price INT	
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------
# MAGIC ##### RETURNS

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dev.spark_db.returns(
# MAGIC 	order_id INT,
# MAGIC 	customerid INT,
# MAGIC 	productid INT,
# MAGIC 	product_name STRING,
# MAGIC 	RETURN_DATE DATE
# MAGIC );

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC --INSERT INTO dev.spark_db.employee
# MAGIC --SELECT * FROM csv

# COMMAND ----------




# COMMAND ----------

# MAGIC %md
# MAGIC ### s
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC query = """
# MAGIC SELECT id, source
# MAGIC from df 
# MAGIC """
# MAGIC
# MAGIC result = duckdb.query(query).df()
# MAGIC print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC 3 Find duplicates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC SELECT * , row_number() over(order by clarity, )
# MAGIC FROM dev.spark_db.diamonds as D
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read a table with spark

# COMMAND ----------

# MAGIC %md
# MAGIC df= spark.table("dev.spark_db.employee")
# MAGIC