# Databricks notebook source
# MAGIC %md
# MAGIC ## Agenda
# MAGIC
# MAGIC #### 1 Create spark session
# MAGIC
# MAGIC   - Create spark session
# MAGIC   - installing some libraries
# MAGIC   - Restarting python (in case previous libraries installation asks to do so)
# MAGIC   - Importing some libraries
# MAGIC
# MAGIC #### 2 create dataframe
# MAGIC
# MAGIC -     1 By reading a file in the Volume(a csv, json, etc.) FOOL(Format, Option, Option, Load)
# MAGIC     -  using  spark.read.format().option().option().load()
# MAGIC - 2 By reading a table where the data sits using spark.read.table
# MAGIC - 3 Check the notebook "3_Customer_Dataframe_Creation".  using spark.createDataFrame(data=data_list, schema=data_schema)
# MAGIC
# MAGIC ###### 2.1 Eyeball to spot possible data issues
# MAGIC
# MAGIC ###### 2.2 Fix any issues discovered
# MAGIC
# MAGIC     Fix data types errors with:
# MAGIC
# MAGIC     Single column    --> .withColumn()     col().cast() or use this expr('cast()')
# MAGIC
# MAGIC     Multiple columns --> .withColumns({ }) col().cast() or use this expr('cast()')
# MAGIC
# MAGIC     example: 
# MAGIC        # Fix the order_date as it was incorrectly inferred
# MAGIC        df_fixed2 =  df2_raw.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")  ) 
# MAGIC
# MAGIC        # FIX MORE THAN ONE COLUMN at the same time
# MAGIC        #-------------------OPTION ONE --Using col()----------------
# MAGIC        #df_fixed2 =  df2_raw.withColumns({"order_date": to_date(col("order_date"), "yyyy-MM-dd"),
# MAGIC        #                                  "order_id": col("order_id").cast("string")
# MAGIC        #                               #"order_id": col("order_id").try_cast(IntegerType()) try_cast returns null if an error hapens    
# MAGIC        #                                 })
# MAGIC
# MAGIC        #-------------------OPTION TWO --Using expr()----------------
# MAGIC        #df_fixed2 =  df2_raw.withColumns({"order_date": to_date(col("order_date"), "yyyy-MM-dd"),
# MAGIC        #                                  "order_id": expr("cast(order_id as string)")    
# MAGIC        #                                 })
# MAGIC
# MAGIC
# MAGIC
# MAGIC #### 3 Query the data the SQL Query way and the PySpark Transformation way
# MAGIC   -   SQL Query way
# MAGIC   -   PySpark transformations way
# MAGIC
# MAGIC
# MAGIC #### 4 Example
# MAGIC
# MAGIC --------------------------------------
# MAGIC
# MAGIC ##### Step One: Creating spark session
# MAGIC
# MAGIC  spark.version
# MAGIC
# MAGIC ##### Step Two: Creating datframe
# MAGIC First of all you must create a dataframe and here we list 3 different ways, although we will describe 2 in this notebook, the 3rd one is discussed in a previous nortebook.
# MAGIC
# MAGIC  1 create a Dataframe by reading a file in the Volume(a csv, json, etc.)
# MAGIC
# MAGIC -     using spark.read.format
# MAGIC                       .option
# MAGIC                       .option
# MAGIC                       .load
# MAGIC
# MAGIC 2 Create a dataframe by reading a table where the data sits
# MAGIC
# MAGIC -     using spark.read.table
# MAGIC
# MAGIC 3 Check the notebook "3_Customer_Dataframe_Creation". We are not reviewing this one for this practice
# MAGIC
# MAGIC -     using spark.createDataFrame(data=data_list, schema=data_schema)
# MAGIC
# MAGIC
# MAGIC ##### Step Three: Query the data SQL Query way and PySpark Transformations way
# MAGIC ------------------------------------------------------------------------------------------
# MAGIC ###### SQL Query way --> Write a normal SQL query 
# MAGIC
# MAGIC ###### PySpark transformations way --> IDEAL to perform the steps of an SQL query
# MAGIC
# MAGIC  -    < 1 Read teh data
# MAGIC
# MAGIC  -    < 2 Apply transformations(querying the data)
# MAGIC         - IDEAL :    ENCAPSULATES ALL TRANSFORMATIONS INTO ONE dataframe
# MAGIC         - NOT IDEAL: Creates a dataframe per transformation
# MAGIC
# MAGIC  -    < 3 Show/Execute the result/Actions(applied to teh result)
# MAGIC
# MAGIC ####5 Practices
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032783996.png](./image_1774032783996.png "image_1774032783996.png")
# MAGIC #### 1 Create spark session
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating spark session

# COMMAND ----------

# We will use this to create a spark session from here onwards
spark.version

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### installing some libraries

# COMMAND ----------

pip install duckdb pandas # install duckdb and pandas to be able to query a dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------
# MAGIC #####Restarting python 

# COMMAND ----------

dbutils.library.restartPython() # tHIS restarts the kernel or python after running the above command to install duckdb

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------
# MAGIC #####Importing some libraries

# COMMAND ----------

import pandas as pd
import duckdb
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import to_date, col

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC ### 2 create dataframe 
# MAGIC
# MAGIC ##### (by reading a file from Volume (csv, json, etc.) )
# MAGIC

# COMMAND ----------

# Reading a csv file
#file_df = ( spark.read.format('csv')
#                      .option('header', 'true')
#                      .option('inferSchema', 'true')
#                      .load(path="/Volumes/dev/spark_db/datasets/spark_programming/data/sf-fire-calls.csv")
#          )

#Read a json file
#Using a connector(options).
#json_file_df = (
#                spark.read.format('json')
#                .load(path= '/Volumes/dev/spark_db/datasets/spark_programming/data/diamonds.json')
#              )


# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------------------------------------------
# MAGIC ##### <> using spark.read.format   (FOOL)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC  <> create a Dataframe by reading a file (in the Volume a csv, json, etc.) 
# MAGIC -     using spark.read.format
# MAGIC                       .option
# MAGIC                       .option
# MAGIC                       .load         

# COMMAND ----------


raw_cust_df = ( spark.read.format('csv')
                          .option("header", True)
                          .option("inferSchema", True)
                          .load("/Volumes/dev/spark_db/datasets/spark_programming/data/customers.csv")
              )

#raw_cust_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### 2.1 Eyeball the data to spot possible issues

# COMMAND ----------

# MAGIC %md
# MAGIC ------------------------------------------------------------------
# MAGIC #####2.2 Fix any data issues discovered.
# MAGIC After reading the data we noticed that no issues so no actions needed

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC  In case you need to make fixes you can make use of these : 
# MAGIC
# MAGIC  From PySpark use either withColumn() or withColumns()
# MAGIC
# MAGIC 1 withColumn() to add a column or replacing the existing column that has the same name. 
# MAGIC
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumn.html
# MAGIC
# MAGIC 2 withColumns() to add multiple columns or replacing the existing columns that have the same names.
# MAGIC
# MAGIC doc: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC ### 3 Query  the data the SQL Query way and the PySpark Transformation way
# MAGIC
# MAGIC ##### Before anything you MUST create a dataframe
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------------------------

# COMMAND ----------

spark.version

# COMMAND ----------

pip install duckdb pandas

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
import duckdb
from pyspark.sql.functions import col, expr
from pyspark.sql.types     import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

raw_cust_df = ( spark.read.format('csv')
                          .option("header", True)
                          .option("inferSchema", True)
                          .load("/Volumes/dev/spark_db/datasets/spark_programmingHBL/data/customers.csv")
              )

#raw_cust_df.display()

raw_ord_df = ( spark.read.format('csv')
                          .option("header", True)
                          .option("inferSchema", True)
                          .load("/Volumes/dev/spark_db/datasets/spark_programmingHBL/data/orders.csv")
              )


raw_prod_df = ( spark.read.format('csv')
                          .option("header", True)
                          .option("inferSchema", True)
                          .load("/Volumes/dev/spark_db/datasets/spark_programmingHBL/data/products.csv")
              )


raw_ret_df = ( spark.read.format('csv')
                          .option("header", True)
                          .option("inferSchema", True)
                          .load("/Volumes/dev/spark_db/datasets/spark_programmingHBL/data/returns.csv")
              )


raw_emp_df = ( spark.read.format('csv')
                          .option("header", True)
                          .option("inferSchema", True)
                          .load("/Volumes/dev/spark_db/datasets/spark_programming/data/employee.csv")
              )

raw_dep_df = ( spark.read.format('csv')
                         .option("header", True)
                         .option("inferSchema", True)
                         .load("/Volumes/dev/spark_db/datasets/spark_programming/data/department.csv")
              )



# COMMAND ----------

27# Option One reading csv file from Volume

#raw_cust_df.display()
#raw_ord_df.display()
#raw_prod_df.display()
#raw_ret_df.display()

#raw_emp_df.display()

#raw_dep_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032783996.png](./image_1774032783996.png "image_1774032783996.png")
# MAGIC
# MAGIC #### 10-300 SQL Interview Questions 
# MAGIC
# MAGIC https://www.linkedin.com/posts/savi-verma-03878520a_300-real-sql-interview-qa-ugcPost-7472676854550933504-cnF1/?utm_source=share&utm_medium=member_desktop&rcm=ACoAAA3LNfoBAE02Gyxbr3zcLMjVYBKkCuChfr0
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### ## TBD
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC FROM   dev.spark_db.customers as c
# MAGIC
# MAGIC
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark

# COMMAND ----------

from pyspark.sql.functions import col, expr, max, count, min, sum, avg,year, month, dense_rank, desc, lag, countDistinct
from pyspark.sql.window import Window
from pyspark.sql.types     import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 1: Using DataFrame API (Recommended)
# MAGIC This approach is the most common and "PySpark way" to achieve the result.

# COMMAND ----------


cust_df = ( raw_cust_df.
          )

cust_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Other ways

# COMMAND ----------




# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 2: Using Spark SQL Temporary View
# MAGIC
# MAGIC You can also use raw SQL directly within PySpark by registering your DataFrame as a temporary view.

# COMMAND ----------

from pyspark.sql.functions import expr, col, count

# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame
raw_cust_df.createOrReplaceTempView("customers")

# Run the SQL query using spark.sql()
sql_query = """

"""
result_df_sql = spark.sql(sql_query)

# Show the result
result_df_sql.show()


# COMMAND ----------

# MAGIC %md
# MAGIC #### ## TBD
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC FROM   dev.spark_db.customers as c
# MAGIC
# MAGIC
# MAGIC ;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark

# COMMAND ----------

from pyspark.sql.functions import col, expr, max, count, min, sum, avg,year, month, dense_rank, desc, lag, countDistinct
from pyspark.sql.window import Window
from pyspark.sql.types     import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 1: Using DataFrame API (Recommended)
# MAGIC This approach is the most common and "PySpark way" to achieve the result.

# COMMAND ----------


cust_df = ( raw_cust_df.
          )

cust_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Other ways

# COMMAND ----------




# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 2: Using Spark SQL Temporary View
# MAGIC
# MAGIC You can also use raw SQL directly within PySpark by registering your DataFrame as a temporary view.

# COMMAND ----------

from pyspark.sql.functions import expr, col, count

# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame
raw_cust_df.createOrReplaceTempView("customers")

# Run the SQL query using spark.sql()
sql_query = """

"""
result_df_sql = spark.sql(sql_query)

# Show the result
result_df_sql.show()


# COMMAND ----------

# MAGIC %md
# MAGIC #### ## TBD
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC FROM   dev.spark_db.customers as c
# MAGIC
# MAGIC
# MAGIC ;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark

# COMMAND ----------

from pyspark.sql.functions import col, expr, max, count, min, sum, avg,year, month, dense_rank, desc, lag, countDistinct
from pyspark.sql.window import Window
from pyspark.sql.types     import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 1: Using DataFrame API (Recommended)
# MAGIC This approach is the most common and "PySpark way" to achieve the result.

# COMMAND ----------


cust_df = ( raw_cust_df.
          )

cust_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Other ways

# COMMAND ----------




# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 2: Using Spark SQL Temporary View
# MAGIC
# MAGIC You can also use raw SQL directly within PySpark by registering your DataFrame as a temporary view.

# COMMAND ----------

from pyspark.sql.functions import expr, col, count

# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame
raw_cust_df.createOrReplaceTempView("customers")

# Run the SQL query using spark.sql()
sql_query = """

"""
result_df_sql = spark.sql(sql_query)

# Show the result
result_df_sql.show()


# COMMAND ----------

# MAGIC %md
# MAGIC #### 36  Find the difference between current row's sales and previous row's sales by product
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_date
# MAGIC       ,product_name
# MAGIC       ,SUM(quantity * price) as sales
# MAGIC       ,LAG(SUM(quantity * price)) OVER(PARTITION BY product_name ORDER BY order_date) AS PREV_SALES
# MAGIC       ,SUM(quantity * price) - LAG(SUM(quantity * price)) OVER(PARTITION BY product_name ORDER BY order_date) AS sales_diff
# MAGIC FROM   dev.spark_db.customers as c
# MAGIC GROUP BY order_date, product_name
# MAGIC ORDER BY product_name, order_date
# MAGIC
# MAGIC
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark

# COMMAND ----------

from pyspark.sql.functions import col, expr, max, count, min, sum, avg,year, month, dense_rank, desc, lag, countDistinct
from pyspark.sql.window import Window
from pyspark.sql.types     import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 1: Using DataFrame API (Recommended)
# MAGIC This approach is the most common and "PySpark way" to achieve the result.

# COMMAND ----------


grouped_date = (raw_cust_df.groupBy("order_date", "product_name")
                           .agg( expr('sum(quantity * price) as Sales') )
               )

windowSpec= Window.partitionBy("product_name").orderBy("order_date")

cust_df = ( grouped_date.withColumns({"Prev_sales": lag('Sales').over(windowSpec),
                                      "Sales_diff": expr('Sales - Prev_sales')
                                    })
                        .orderBy("product_name", "order_date")

         )

cust_df.display()       

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Other ways

# COMMAND ----------




# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 2: Using Spark SQL Temporary View
# MAGIC
# MAGIC You can also use raw SQL directly within PySpark by registering your DataFrame as a temporary view.

# COMMAND ----------

from pyspark.sql.functions import expr, col, count

# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame
raw_cust_df.createOrReplaceTempView("customers")

# Run the SQL query using spark.sql()
sql_query = """

"""
result_df_sql = spark.sql(sql_query)

# Show the result
result_df_sql.show()


# COMMAND ----------

# MAGIC %md
# MAGIC #### 38: Find the avearge order value by month and by product
# MAGIC ----------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SQL query

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --WITH T1 AS (
# MAGIC         SELECT  EXTRACT(MONTH FROM ORDER_DATE) AS MTH, PRODUCT_NAME, AVG(QUANTITY * PRICE) AS AVG_VALUE
# MAGIC         FROM dev.spark_db.customers
# MAGIC         GROUP BY EXTRACT(MONTH FROM ORDER_DATE), PRODUCT_NAME
# MAGIC         ORDER BY MTH, AVG_VALUE DESC
# MAGIC         --FROM dev.spark_db.employee
# MAGIC  --          )
# MAGIC --SELECT CUSTOMER_NAME, COUNT(DISTINCT MTH) AS CT
# MAGIC --FROM T1 
# MAGIC
# MAGIC ;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 1: Using DataFrame API (Recommended)
# MAGIC This approach is the most common and "PySpark way" to achieve the result.

# COMMAND ----------

from pyspark.sql.functions import col, expr, max, count, min, sum, avg,year, month, dense_rank, desc, lag, countDistinct, date_diff, lit
from pyspark.sql.window import Window
from pyspark.sql.types     import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------


cust_df = ( raw_cust_df.withColumn("MTH", expr("month(order_date)"))
                       .groupBy("MTH", "product_name")
                       .agg( expr('avg(quantity* price)' ).alias("AVG_VALUE" ) )
                       .select("MTH", "product_name", "AVG_VALUE")
                       .orderBy("MTH", "AVG_VALUE", ascending=[True, False])

          ) 
cust_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Other ways

# COMMAND ----------


# Using the native PySpaek DataFrame API


cust_df = (  raw_cust_df
     )

cust_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 2: Using Spark SQL Temporary View
# MAGIC
# MAGIC You can also use raw SQL directly within PySpark by registering your DataFrame as a temporary view.

# COMMAND ----------

from pyspark.sql.functions import expr, col, count

# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame
raw_emp_df.createOrReplaceTempView("employee")

# Run the SQL query using spark.sql()
sql_query = """

;
"""
result_df = spark.sql(sql_query)

# Show the result
result_df.show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### ## TBD
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC FROM   dev.spark_db.customers as c
# MAGIC
# MAGIC
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark

# COMMAND ----------

from pyspark.sql.functions import col, expr, max, count, min, sum, avg,year, month, dense_rank, desc, lag, countDistinct
from pyspark.sql.window import Window
from pyspark.sql.types     import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 1: Using DataFrame API (Recommended)
# MAGIC This approach is the most common and "PySpark way" to achieve the result.

# COMMAND ----------


cust_df = ( raw_cust_df.
          )

cust_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Other ways

# COMMAND ----------




# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 2: Using Spark SQL Temporary View
# MAGIC
# MAGIC You can also use raw SQL directly within PySpark by registering your DataFrame as a temporary view.

# COMMAND ----------

from pyspark.sql.functions import expr, col, count

# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame
raw_cust_df.createOrReplaceTempView("customers")

# Run the SQL query using spark.sql()
sql_query = """

"""
result_df_sql = spark.sql(sql_query)

# Show the result
result_df_sql.show()


# COMMAND ----------

# MAGIC %md
# MAGIC #### ## TBD
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC FROM   dev.spark_db.customers as c
# MAGIC
# MAGIC
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark

# COMMAND ----------

from pyspark.sql.functions import col, expr, max, count, min, sum, avg,year, month, dense_rank, desc, lag, countDistinct
from pyspark.sql.window import Window
from pyspark.sql.types     import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 1: Using DataFrame API (Recommended)
# MAGIC This approach is the most common and "PySpark way" to achieve the result.

# COMMAND ----------


cust_df = ( raw_cust_df.
          )

cust_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Other ways

# COMMAND ----------




# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 2: Using Spark SQL Temporary View
# MAGIC
# MAGIC You can also use raw SQL directly within PySpark by registering your DataFrame as a temporary view.

# COMMAND ----------

from pyspark.sql.functions import expr, col, count

# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame
raw_cust_df.createOrReplaceTempView("customers")

# Run the SQL query using spark.sql()
sql_query = """

"""
result_df_sql = spark.sql(sql_query)

# Show the result
result_df_sql.show()


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ---------------------------------------------------------------
# MAGIC ### UNTIL HERE
# MAGIC ---------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ####37: TBD
# MAGIC ----------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SQL query

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --WITH REV_YEAR AS (
# MAGIC                      SELECT
# MAGIC                      FROM  dev.spark_db.customers
# MAGIC                      GROUP BY 
# MAGIC                     )      
# MAGIC  
# MAGIC --SELECT 
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 1: Using DataFrame API (Recommended)
# MAGIC This approach is the most common and "PySpark way" to achieve the result.

# COMMAND ----------

from pyspark.sql.functions import col, expr, max, count, min, sum, avg,year, month, dense_rank, desc, lag, countDistinct, date_diff, lit
from pyspark.sql.window import Window
from pyspark.sql.types     import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ###### Other ways

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 2: Using Spark SQL Temporary View
# MAGIC
# MAGIC You can also use raw SQL directly within PySpark by registering your DataFrame as a temporary view.

# COMMAND ----------

from pyspark.sql.functions import expr, col, count

# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame
raw_emp_df.createOrReplaceTempView("employee")

# Run the SQL query using spark.sql()
sql_query = """

;
"""
result_df = spark.sql(sql_query)

# Show the result
result_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####38: TBD
# MAGIC ----------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SQL query

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --WITH REV_YEAR AS (
# MAGIC                      SELECT
# MAGIC                      FROM  dev.spark_db.customers
# MAGIC                      GROUP BY 
# MAGIC                     )      
# MAGIC  
# MAGIC --SELECT 
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 1: Using DataFrame API (Recommended)
# MAGIC This approach is the most common and "PySpark way" to achieve the result.

# COMMAND ----------

from pyspark.sql.functions import col, expr, max, count, min, sum, avg,year, month, dense_rank, desc, lag, countDistinct, date_diff, lit
from pyspark.sql.window import Window
from pyspark.sql.types     import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ###### Other ways

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 2: Using Spark SQL Temporary View
# MAGIC
# MAGIC You can also use raw SQL directly within PySpark by registering your DataFrame as a temporary view.

# COMMAND ----------

from pyspark.sql.functions import expr, col, count

# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame
raw_emp_df.createOrReplaceTempView("employee")

# Run the SQL query using spark.sql()
sql_query = """

;
"""
result_df = spark.sql(sql_query)

# Show the result
result_df.show()