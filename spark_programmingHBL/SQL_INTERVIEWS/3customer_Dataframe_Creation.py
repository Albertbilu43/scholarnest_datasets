# Databricks notebook source
# MAGIC %md
# MAGIC ## Agenda
# MAGIC
# MAGIC #####1 Create spark session
# MAGIC
# MAGIC #####2 create dataframe
# MAGIC
# MAGIC   Using spark.createDataFrame(data=, schema=)
# MAGIC
# MAGIC   * customers
# MAGIC   * orders
# MAGIC   
# MAGIC     Fix data types errors with:
# MAGIC
# MAGIC     Single column    --> .withColumn()     col().cast() or use this expr('cast()')
# MAGIC
# MAGIC     Multiple columns --> .withColumns({ }) col().cast() or use this expr('cast()')
# MAGIC
# MAGIC
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1 Create spark session
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

# The pyspark.sql.types module should be used when you need to explicitly define the schema of your data in a PySpark application.
# pyspark.sql.functions module should be used to perform efficient, optimized, and scalable data transformations, filtering, and aggregations on large datasets within the PySpark DataFrame API.

import pandas as pd
import duckdb
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import to_date, col

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create dataframe: customer

# COMMAND ----------

# Sample data with different date string formats and a timestamp string

data_schema = StructType([
                         StructField("customerid", IntegerType(), True),
                         StructField("customer_name", StringType(), True),
                         StructField("order_id", IntegerType(), True),              
                         StructField("productid", IntegerType(), True),     
                         StructField("product_name", StringType(), True), 
                         StructField("quantity", IntegerType(), True),              
                         StructField("price", IntegerType(), True), 
                         StructField("order_date", StringType(), True) 
                         # changed "order_date" from DATE to String to be able to apply withcolumn() function
                         # Fix date format by first adding '' to each date to convert date into string and then apply the function to change back to date due to 
                         # an isse of teh pyspark version 4 not reading the date input and changing all dates to 1975
                       ])


data_list  = [
	(1, 'Alice'   ,2201, 101, 'Widget', 2, 50 , '2022-02-27'),
	(2, 'Bob'     ,2202, 102, 'Gadget', 1, 150 , '2020-01-12'),
	(3, 'Carol'   ,2203, 103, 'Doodad', 5, 4  , '2019-11-17'),
	(4, 'Dave'    ,2129, 101, 'Widget', 10, 50 , '2020-12-23'),
	(3, 'Carol'   ,2300, 103, 'Doodad', 2,   8 , '2020-04-01'),

  (5, 'Lewis'   ,2342, 102, 'Gadget', 1, 150 , '2024-01-01'),
  (5, 'Lewis'   ,2343, 102, 'Gadget', 2, 150 , '2024-02-01'),
  (5, 'Lewis'   ,2344, 102, 'Gadget', 2, 150 , '2024-03-01'),
  (5, 'Lewis'   ,2345, 102, 'Gadget', 2, 150 , '2024-04-01'),
  (5, 'Lewis'   ,2346, 102, 'Gadget', 2, 150 , '2024-05-01'),
  (5, 'Lewis'   ,2347, 102, 'Gadget', 2, 150 , '2024-06-01'),
	(5, 'Lewis'   ,2348, 102, 'Gadget', 2, 150 , '2024-07-01'),	
  (5, 'Lewis'   ,2349, 102, 'Gadget', 3, 150 , '2024-07-02'),  
  (5, 'Lewis'   ,2350, 102, 'Gadget', 3, 150 , '2024-08-21'),   
  (5, 'Lewis'   ,2351, 102, 'Gadget', 3, 150 , '2024-09-21'),
  (5, 'Lewis'   ,2352, 102, 'Gadget', 3, 150 , '2024-10-30'),  
  (5, 'Lewis'   ,2353, 102, 'Gadget', 3, 150 , '2024-11-21'),   
  (5, 'Lewis'   ,2354, 102, 'Gadget', 3, 150 , '2024-12-21'),
  (5, 'Lewis'   ,2355, 102, 'Gadget', 3, 150 , '2025-11-21'),
	(6, 'Gillian' ,2360, 101, 'Widget', 4,  50 , '2025-09-11'),
	(6, 'Gillian' ,2361, 105, 'Office Suite', 3,  600 , '2025-11-11'),
	
	(2, 'Bob'     ,2362, 102, 'Gadget', 3, 150 , '2026-01-02'),
	(3, 'Carol'   ,2363, 103, 'Doodad', 20,  4 , '2026-02-17'),
	(4, 'Dave'    ,2364, 101, 'Widget', 8,  50 , '2026-03-03'),
	(4, 'Dave'    ,2365, 101, 'Widget', 30, 50 , '2026-03-04'),
	(3, 'Carol'   ,2366, 103, 'Doodad', 20,  8 , '2026-03-11')
             ]
df_raw = spark.createDataFrame(data=data_list, schema=data_schema)

# fix date for ONLY ONE column that is why I used withcolumn() function.
df_fixed =  df_raw.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")  ) 

df_fixed.display()






# COMMAND ----------

# MAGIC %md
# MAGIC ### Create dataframe: orders

# COMMAND ----------

# DBTITLE 1,Untitled
from pyspark.sql.functions import col, to_date, expr


data_schema2 = StructType([
                          StructField("order_id", IntegerType(), True),
                          StructField("customerid", IntegerType(), True),              
                          StructField("productid", IntegerType(), True),              
                          StructField("product_name", StringType(), True),
                          StructField("region", StringType(), True),
                          StructField("order_date", StringType(), True) 
                          # Fix "order_date" date format by first adding '' to convert date into string and then to date due to an isse of teh pyspark version 4 not reading the date input and changing all dates to 1975
                         ])

data_list2 = [
	(2201, 1, 101, 'Widget', 'Central', '2022-02-27'),
	(2202, 2, 102, 'Gadget', 'North','2020-01-12'),
	(2203, 3, 103, 'Doodad', 'South','2019-11-17'),
	(2129, 4, 101, 'Widget', 'West','2020-12-23'),
	(2300, 3, 103, 'Doodad', 'West','2020-04-01'),
	
	(2342, 5, 102, 'Gadget', 'West','2024-01-01'),
	(2343, 5, 102, 'Gadget', 'West','2024-02-01'),
	(2344, 5, 102, 'Gadget', 'West','2024-03-01'),
	(2345, 5, 102, 'Gadget', 'West','2024-04-01'),
	(2346, 5, 102, 'Gadget', 'West','2024-05-01'),
	(2347, 5, 102, 'Gadget', 'West','2024-06-01'),
	(2348, 5, 102, 'Gadget', 'West','2024-07-01'),
	(2349, 5, 102, 'Gadget', 'West','2024-07-02'),
	(2350, 5, 102, 'Gadget', 'West','2024-08-21'),	
	(2351, 5, 102, 'Gadget', 'West','2024-09-21'),
	(2352, 5, 102, 'Gadget', 'West','2024-10-30'),
	(2353, 5, 102, 'Gadget', 'West','2024-11-21'),	
	(2354, 5, 102, 'Gadget', 'West','2024-12-21'),
	(2355, 5, 102, 'Gadget', 'West','2025-11-21'),
	(2360, 6, 101, 'Widget', 'South','2025-09-11'),
	(2361, 6, 105, 'Office Suite', 'South','2025-11-11'),
	
	(2362, 2, 102, 'Gadget', 'North','2026-01-02'),
	(2363, 3, 103, 'Doodad', 'South', '2026-02-17'),
	(2364, 4, 101, 'Widget', 'West', '2026-03-03'),
	(2365, 4, 101, 'Widget', 'West', '2026-03-04'),	
	(2366, 3, 103, 'Doodad', 'West', '2026-03-11')	
            ]


df2_raw = spark.createDataFrame(data=data_list2, schema=data_schema2)

# Fix the order_date as it was incorrectly inferred
df_fixed2 =  df2_raw.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")  ) 

# FIX MORE THAN ONE COLUMN at the same time
#-------------------OPTION ONE --Using col()----------------
#df_fixed2 =  df2_raw.withColumns({"order_date": to_date(col("order_date"), "yyyy-MM-dd"),
#                                  "order_id": col("order_id").cast("string")
#                                 })

#-------------------OPTION TWO --Using expr()----------------
#df_fixed2 =  df2_raw.withColumns({"order_date": to_date(col("order_date"), "yyyy-MM-dd"),
#                                  "order_id": expr("cast(order_id as string)")
#                                 })


df_fixed2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create dataframe: product

# COMMAND ----------

# DBTITLE 1,Untitled

data_schema3 = StructType([
                           StructField("productid", IntegerType(), True),
                           StructField("product_name", StringType(), True),
                           StructField("unit_product_price", IntegerType(), True)
                          ])


data_list3  = [
	             (101, 'Widget', 50),
	             (102, 'Gadget', 150),
	             (103, 'Doodad', 4),
	             (104, 'Antivirus', 130),
	             (105, 'Office Suite', 600)	
	
              ]
             
df3 = spark.createDataFrame(data=data_list3, schema=data_schema3)

df3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create dataframe: returns

# COMMAND ----------



data_schema4 = StructType([
                           StructField("order_id", IntegerType(), True),
                           StructField("customerid", IntegerType(), True),              
                           StructField("productid", IntegerType(), True), 
                           StructField("product_name", StringType(), True),
                           StructField("RETURN_DATE", StringType(), True)  
                           # Fix date format by first adding "" to convert date into string and then to date due to an isse of teh pyspark version 4 not reading the date input and changing all dates to 1975
                          ])


data_list4  = [ 	(2202, 2, 102, 'Gadget', '2020-01-25'),
                    (2344, 5, 102, 'Gadget', '2024-04-02'),
                    (2345, 5, 102, 'Gadget', '2024-04-14'),	
	                (2366, 3, 103, 'Doodad', '2026-03-13')
             ]
df4_raw = spark.createDataFrame(data=data_list4, schema=data_schema4)

# Fix date format
df_fixed4 =  df4_raw.withColumn("RETURN_DATE", to_date(col("RETURN_DATE"), "yyyy-MM-dd")  ) 

df_fixed4.display()


# COMMAND ----------




# COMMAND ----------

