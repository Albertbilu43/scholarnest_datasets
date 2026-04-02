# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC Upload the csv file to the volume
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Agenda
# MAGIC ### 1 Create a Spark Session
# MAGIC -    Using Builder
# MAGIC -    Using Pre-created session
# MAGIC
# MAGIC ### 2 Create a DataFrame by reading the data file (from the Volume)
# MAGIC   2.1 Eyeball to spot possible data issues
# MAGIC
# MAGIC   2.2 Fix any issues discovered
# MAGIC
# MAGIC ### 3 Load data to previously created tables
# MAGIC
# MAGIC Example:
# MAGIC ret_df.write.mode("overwrite").saveAsTable("dev.spark_db.returns")

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1773963100728.png](./image_1773963100728.png "image_1773963100728.png")
# MAGIC ![image_1773963072158.png](./image_1773963072158.png "image_1773963072158.png")
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1 Create a Spark Session
# MAGIC
# MAGIC ##### Using a Builder

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC from pyspark.sql import SparkSession
# MAGIC
# MAGIC spark_session = SparkSession.builder.getOrCreate()
# MAGIC
# MAGIC spark_session.version

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Using a pre-created Spark Session

# COMMAND ----------

#This below is a pre-created spark session. It's used when you want t avoid creating the spark session with teh builder. "spark" is teh precreated session
# We will use this to create a spark session from here onwards
spark.version

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1773963120159.png](./image_1773963120159.png "image_1773963120159.png")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ####2 Create a DataFrame by reading the data file (from the Volume) 
# MAGIC ###### FOOL(Format, Option, Option, Load) 
# MAGIC spark.read.format().option().option().load()
# MAGIC
# MAGIC  Customer, Orders, Product, Returns

# COMMAND ----------

"""
spark.read.  Returns a DataFrameReader that can be used to read data in a DataFrame. 
https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.read.html

spark.read.format Specifies the file type (can be "json", "parquet", "jdbc", "orc", "text", etc.)..
https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.format.html#

spark.read.option  Adds an input option for the underlying data source.
https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.option.html

spark.read.load   Loads the data from the specified path(s)
https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.load.html




"""

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Customer table 
# MAGIC How to use SparkSession to read a csv file 
# MAGIC
# MAGIC

# COMMAND ----------

raw_cust_df= (
          spark.read.format('csv')
                    .option("header", "true")        # Use the first line as the column names
                    .option("inferSchema", "true")   # Automatically detect column data types e.i if a column contains names then infer is String
                    .load("/Volumes/dev/spark_db/datasets/spark_programming/data/customers.csv") # Read the file from this location
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####2.1 Eyeball the data to spot possible issues
# MAGIC
# MAGIC At a first glance no data issues need to be fixed. The data content matches types
# MAGIC

# COMMAND ----------

#raw_cust_df.display()
#raw_cust_df.printSchema()


# COMMAND ----------

# MAGIC %md
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

# DBTITLE 1,Fix departmentid and enddate types
from pyspark.sql.functions import expr, col

cust_df = raw_cust_df
cust_df.display()


""" 

The core difference between PySpark's
select() and selectExpr() is in their input type and syntax: 
  -select() uses the DataFrame API's column objects and functions
  -selectExpr() accepts SQL-style expressions as strings.


expr(str)          - This is useful when you want to apply a SQL expression to a DataFrame column or perform a SQL-like operation on a DataFrame.
                     Allows users to write SQL expressions using the syntax they know (e.g., length(name), Balance * 1.05 AS Updated_Balance, 
                     CASE WHEN...). 
                     Allows you to evaluate a string containing a SQL-style expression directly within DataFrame operations.
                     Example: # 1. Create a new column using a SQL expression
                                   df_with_length = df.select("*", expr("length(name) AS name_length"))

<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<expr() vs. selectExpr()>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>


*expr()              operates on a single SQL expression and returns a Column object for use within other DataFrame methods, 
*selectExpr()        Accepts one or more SQL expressions as strings and applies them across the entire DataFrame in a single call, returning a new
                     DataFrame. 
                     <<python>>
                     # Using selectExpr to perform multiple operations at once
                     df_transformed = df.selectExpr( "name",
                                                     "Balance * 1.02 AS Adjusted_Balance",
                                                      "CASE WHEN Balance > 100000 THEN 'High' ELSE 'Low' END AS Balance_Level"
                                                    )

col(col)           - Se utiliza para hacer referencia a una columna de un DataFrame por su nombre, devolviendo un objeto de tipo Column. 
                     Es fundamental para transformaciones, filtros y ordenaciones al convertir cadenas de texto en expresiones de columna. 
                     col("nombre_columna").
column(col)        - Returns a Column based on the given column name.
lit(col)           - Creates a Column of literal value.
try_cast(dataType) - A special version of cast that performs the same operation, but returns a NULL value instead of raising an error if 
                     the invoke method throws exception.
"""



# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1773963136344.png](./image_1773963136344.png "image_1773963136344.png")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load data into the Customers table

# COMMAND ----------

"""
.write -  Writing data in PySpark is an action performed on a DataFrame using .write, which provides methods to save data to external storage.
          Is not a standard, built-in PySpark function. 
write.mode - is used to specify how to handle existing data when saving a DataFrame to an external source (like a file system or database table)
             Specifies the behavior when data already exists in the target location. 
             The default mode is "error", which raises an error if the target already exists.
             SYNTAX is dataframe.write.mode("mode_name").save(path)
             exmaple:    cust_df.write.mode("overwrite").saveAsTable()
    
"""


# COMMAND ----------

cust_df.write.mode("overwrite").saveAsTable("dev.spark_db.customers")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Query the table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) FROM dev.spark_db.customers  -- count all records
# MAGIC
# MAGIC --df = spark.table("dev.spark_db.customers")
# MAGIC --df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####5. orders table 
# MAGIC How to use SparkSession to read a csv file
# MAGIC
# MAGIC ##### At a first glance data looks fine

# COMMAND ----------

raw_ord_df= (
              spark.read.format('csv')
                   .option("header", "true")
                   .option("inferSchema", "true")
                   .load("/Volumes/dev/spark_db/datasets/spark_programming/data/orders.csv")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Data is fine so no fixes needed.
# MAGIC

# COMMAND ----------

ord_df= raw_ord_df
ord_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Load data into department table

# COMMAND ----------

# DBTITLE 1,Untitled
ord_df.write.mode("overwrite").saveAsTable("dev.spark_db.orders")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query the table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) FROM dev.spark_db.orders  -- count all records
# MAGIC
# MAGIC --df = spark.table("dev.spark_db.orders")
# MAGIC --df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####6. PRODUCTS table 
# MAGIC How to use SparkSession to read a csv file
# MAGIC
# MAGIC ##### At a first glance data looks fine

# COMMAND ----------

raw_prod_df= (
              spark.read.format('csv')
                   .option("header", "true")
                   .option("inferSchema", "true")
                   .load("/Volumes/dev/spark_db/datasets/spark_programming/data/products.csv")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Data is fine so no fixes needed

# COMMAND ----------

prod_df= raw_prod_df
prod_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load data into products table

# COMMAND ----------

prod_df.write.mode("overwrite").saveAsTable("dev.spark_db.products")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Query the table

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ####7. returns table 
# MAGIC How to use SparkSession to read a csv file
# MAGIC
# MAGIC ##### At a first glance data looks fine

# COMMAND ----------

raw_ret_df= (
              spark.read.format('csv')
                   .option("header", "true")
                   .option("inferSchema", "true")
                   .load("/Volumes/dev/spark_db/datasets/spark_programming/data/returns.csv")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Data is fine so no fixes needed
# MAGIC

# COMMAND ----------

ret_df= raw_ret_df
ret_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Load data into returns table

# COMMAND ----------

ret_df.write.mode("overwrite").saveAsTable("dev.spark_db.returns")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query the table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM dev.spark_db.returns  -- count all records
# MAGIC
# MAGIC --df = spark.table("dev.spark_db.returns")
# MAGIC --df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>
# MAGIC