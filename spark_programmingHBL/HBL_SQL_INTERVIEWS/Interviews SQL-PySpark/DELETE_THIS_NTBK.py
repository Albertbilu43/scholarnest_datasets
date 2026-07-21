# Databricks notebook source
spark.version

# COMMAND ----------

pip install duckdb pandas

# COMMAND ----------

dbutils.library.restartPython()


# COMMAND ----------

from pyspark.sql.functions import col, expr, max, count, min, sum, avg,year, month, dense_rank, desc, lag, countDistinct
from pyspark.sql.window    import Window
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

# Option One reading csv file from Volume

#raw_cust_df.display()
#raw_ord_df.display()
#raw_prod_df.display()
#raw_ret_df.display()

#raw_emp_df.display()

#raw_dep_df.display()





# COMMAND ----------

# MAGIC %md
# MAGIC ### TBD
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Practices
# MAGIC
# MAGIC #### Query

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC FROM  dev.spark_db.department as d
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, expr, max, count, min, sum, avg,year, month, dense_rank, desc, lag, countDistinct, date_diff, lit
from pyspark.sql.window    import Window
from pyspark.sql.types     import StructType, StructField, StringType, IntegerType, DateType

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



# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Monthly sales revenue and order count
# MAGIC
# MAGIC SELECT extract(MONTH FROM ORDER_DATE) as MTH, SUM(QUANTITY * PRICE) AS SALES, COUNT(DISTINCT ORDER_ID) AS COUNT
# MAGIC FROM   dev.spark_db.customers as c
# MAGIC GROUP BY extract(MONTH FROM ORDER_DATE)
# MAGIC order by MTH
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standard way

# COMMAND ----------

# MAGIC %sql
# MAGIC from pyspark.sql.functions import col, expr, count
# MAGIC
# MAGIC cust_df = ( raw_cust_df.groupBy('month(order_date)')
# MAGIC                        .agg(expr('sum(quantity * price) as SALES'),
# MAGIC                             expr('count(order_id) as CNT')
# MAGIC                            )
# MAGIC                         
# MAGIC           )
# MAGIC
# MAGIC cust_df.display()
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Temporary view

# COMMAND ----------

from pyspark.sql.functions import col, expr, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

raw_emp_df.createOrReplaceTempView("employee")

sql_query = ( """
              WITH T1 AS ( SELECT SALARY , DENSE_RANK() OVER( ORDER BY SALARY DESC) AS RN
                          FROM dev.spark_db.employee
                         GROUP BY SALARY   

                         )

              SELECT  SALARY
              FROM T1
              WHERE RN = 2;
              """
            )
result_df = spark.sql(sql_query)
result_df.display()          


# COMMAND ----------

# MAGIC %md
# MAGIC #### What San Francisco neighborhoods in in the zip codes 94102 and 94103

# COMMAND ----------

# MAGIC %sql
# MAGIC -- City, Neighborhood, Zipcode
# MAGIC
# MAGIC SELECT City, Neighborhood, Zipcode
# MAGIC FROM dev.spark_db.sf_fire_calls
# MAGIC WHERE City = 'SF' and Zipcode in (94102 , 94103);

# COMMAND ----------


from pyspark.sql.window    import Window
from pyspark.sql.functions import rank, col, count, sum, expr, desc

result_df = ( raw_fire_df.where( (raw_fire_df["Zipcode"].isin([94102 , 94103])) & (raw_fire_df["City"]=='SF') )
                         .select("City", "Neighborhood", "Zipcode")
            )

result_df.display()

# COMMAND ----------

