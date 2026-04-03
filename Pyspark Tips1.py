# Databricks notebook source
# MAGIC %md
# MAGIC ##Agenda
# MAGIC
# MAGIC ### 3 Pyspark Tips  
# MAGIC      1-  Methods for dataframes(read, format, etc, )
# MAGIC      2-  Select() vs selectExpr()
# MAGIC      3-  Grouping vs agg()
# MAGIC      4-  where/filter/having
# MAGIC      5-  regexp_replace(), replace()
# MAGIC      6-  cast(), try_cast()
# MAGIC      7-  casting to date fails
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ![image_1774985632190.png](./image_1774985632190.png "image_1774985632190.png")
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 3 Pyspark Tips
# MAGIC
# MAGIC '
# MAGIC
# MAGIC ![image_1774641851028.png](./image_1774641851028.png "image_1774641851028.png")
# MAGIC
# MAGIC -----------------------------------------------------------
# MAGIC #### 1 Methods for dataframes(read, format, etc)
# MAGIC -----------------------------------------------------------
# MAGIC       spark.read.  Returns a DataFrameReader  to read data in a DataFrame. 
# MAGIC       https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.read.html
# MAGIC
# MAGIC       spark.read.format Specifies the file type (can be "json", "parquet", "jdbc", "orc", "text", etc.)..
# MAGIC       https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.format.html#
# MAGIC
# MAGIC       spark.read.option  Adds an input option for the underlying data source.
# MAGIC       https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.option.html
# MAGIC
# MAGIC       spark.read.load   Loads the data from the specified path(s)
# MAGIC       https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.load.html
# MAGIC
# MAGIC
# MAGIC ![image_1774641851028.png](./image_1774641851028.png "image_1774641851028.png")
# MAGIC
# MAGIC -----------------------------------------------------------
# MAGIC #### 2 select() vs selectExtr()
# MAGIC -----------------------------------------------------------
# MAGIC   -     Both methods return a new DataFrame and are used for selecting and transforming columns
# MAGIC
# MAGIC ##### The core difference is that
# MAGIC        * select() uses PySpark's Column API with expressions
# MAGIC        * selectExpr() accepts standard SQL expressions as strings. 
# MAGIC          but CAREFUL you CANNOT include 'where' statement inside it or do any groupings for a particular dimension
# MAGIC          UNLESS you create a TemporaryView first to execute a whole SQL expresion
# MAGIC
# MAGIC -----------------------------------------------------------------------------------------
# MAGIC
# MAGIC select() Key Characteristics: 
# MAGIC
# MAGIC   -  API: Uses Column objects and Column expressions (col("...")).
# MAGIC   -  Use Case: Ideal for selecting specific columns and basic manipulation.
# MAGIC   -  Example: df.select(col("name"), (col("age") + 10).alias("age_plus_10")). 
# MAGIC
# MAGIC selectExpr() Key Characteristics: 
# MAGIC
# MAGIC   -  API: Accepts only SQL expressions formatted as strings.
# MAGIC   -  Use Case: Ideal for rapid SQL-style transformations and alias naming.
# MAGIC   -  Example: df.selectExpr("name", "age + 10 as age_plus_10"). 
# MAGIC
# MAGIC Key Differences:
# MAGIC
# MAGIC   -  Syntax: select() uses Pythonic column expressions; selectExpr() uses pure string SQL.
# MAGIC   -  Flexibility: selectExpr() is more concise for complex SQL transformations (e.g., when, abs, concat) in a single string.
# MAGIC   -  Performance: Generally, both are efficient, but select() is slightly more direct as it avoids parsing SQL strings.
# MAGIC   -  Renaming: selectExpr() makes renaming columns within expressions more concise. 
# MAGIC
# MAGIC .
# MAGIC
# MAGIC
# MAGIC
# MAGIC ##### When to Use Which?
# MAGIC
# MAGIC ######   Use select() 
# MAGIC     when you are building transformations programmatically (e.g., looping through a list of Column objects) or when you prefer a strictly Pythonic API style.
# MAGIC -------------------------------------------------------------    
# MAGIC ######  Use selectExpr() 
# MAGIC     when you want to write concise, SQL-like transformations without importing the functions module for every simple operation. 
# MAGIC     It is also excellent for users already comfortable with SQL syntax
# MAGIC """
# MAGIC #####   <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<expr() vs. selectExpr()>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# MAGIC
# MAGIC
# MAGIC ##### expr()             
# MAGIC  operates on a single SQL expression and returns a Column object for use within other DataFrame methods, 
# MAGIC ##### selectExpr()        
# MAGIC                       Accepts one or more SQL expressions as strings and applies them across the entire DataFrame in a single call,
# MAGIC                       returning a new DataFrame. 
# MAGIC
# MAGIC                       <<python>>
# MAGIC                       # Using selectExpr to perform multiple operations at once
# MAGIC                       
# MAGIC                       -  df_transformed = df.selectExpr( "name", "Balance * 1.02 AS Adjusted_Balance",
# MAGIC                                                         "CASE WHEN Balance > 100000 THEN 'High' ELSE 'Low' END AS Balance_Level"
# MAGIC                                                       )
# MAGIC
# MAGIC #####  col(col)           
# MAGIC                     - Se utiliza para hacer referencia a una columna de un DataFrame por su nombre, devolviendo un objeto de tipo Column. 
# MAGIC                     Es fundamental para:
# MAGIC                     -  transformaciones
# MAGIC                     -  filtros 
# MAGIC                     -  ordenaciones al convertir cadenas de texto en expresiones de columna. 
# MAGIC                        col("nombre_columna").
# MAGIC
# MAGIC #####  column(col)        
# MAGIC                     - Returns a Column based on the given column name.
# MAGIC #####  lit(col)           
# MAGIC                     - Creates a Column of literal value.
# MAGIC #####  try_cast(dataType) 
# MAGIC                     - A special version of cast that performs the same operation, but returns a NULL value instead of raising 
# MAGIC                       an error if the invoke method throws exception.
# MAGIC """
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Examples Renaming a Column (Alias)

# COMMAND ----------

In select(), you must use a Column object to call .alias(). 
# select()
#from pyspark.sql import functions as F
#df.select(F.col("age").alias("current_age"))

In selectExpr(), you use the SQL 'AS' keyword.
# selectExpr()
#df.selectExpr("age AS current_age")


# COMMAND ----------

# MAGIC %md
# MAGIC ###### Examples Mathematical Operations & SQL Functions 
# MAGIC
# MAGIC selectExpr() Is not designed to filter the dataset 
# MAGIC
# MAGIC is often more concise for simple calculations or applying built-in SQL functions like abs(), upper(), or cast().
# MAGIC
# MAGIC

# COMMAND ----------

# select()
#df.select((F.col("age") + 10).alias("age_plus_ten"))

# selectExpr()
#df.selectExpr("age + 10 AS age_plus_ten", "CAST(id AS STRING)")


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ----------------------------------------------------------------------------
# MAGIC ####'where' SQL keyword 
# MAGIC ##### CANNOT BE used directly inside selectExpr() in PySpark to filter rows. 
# MAGIC  
# MAGIC     The selectExpr() method is designed for selecting columns and applying SQL expressions to those 
# MAGIC     columns(e.g., transformations, aggregations, or conditional logic with CASE WHEN), not to filter the dataset  
# MAGIC
# MAGIC
# MAGIC ![image_1774727977650.png](./image_1774727977650.png "image_1774727977650.png")
# MAGIC ![image_1774728759270.png](./image_1774728759270.png "image_1774728759270.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ####To use  GROUP BY operation directly in selectExpr(). First need to register your DataFrame as a temporary SQL view 
# MAGIC
# MAGIC   -     Using selectExper() To calculate the revenue for each productid in PySpark, you first need to register your DataFrame as a temporary SQL view 
# MAGIC         and then execute a SQL query. 
# MAGIC
# MAGIC         PySpark's selectExpr() is designed to run SQL expressions on an existing DataFrame, not to perform a 
# MAGIC         GROUP BY operation directly.
# MAGIC
# MAGIC .

# COMMAND ----------

"""
# 1. Register the DataFrame as a temporary view
df.createOrReplaceTempView("sales_data")

# 2. Use spark.sql() to perform the GROUP BY
result_df = spark.sql("""
                      SELECT category, SUM(amount) as total_sales 
                      FROM sales_data 
                      GROUP BY category
                     """)

"""

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ![image_1774641851028.png](./image_1774641851028.png "image_1774641851028.png")
# MAGIC
# MAGIC -----------------------------------------------------------
# MAGIC #### 3 groupBy() and agg()
# MAGIC -----------------------------------------------------------
# MAGIC .
# MAGIC
# MAGIC #### selectExpr() 
# MAGIC ###### In PySpark  You can use selectExpr() to perform aggregations in two primary ways: 
# MAGIC
# MAGIC   -     1 Grouped Aggregation (after a groupBy()).  Use groupBy() then use agg() or selectExpr() 
# MAGIC   -     2 Global Aggregation: either on the entire DataFrame 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 1. Grouped Aggregation (after groupBy())  <<This is the STANDARD/COMMON way>>
# MAGIC To aggregate data within specific groups, you 
# MAGIC    - first use groupBy() and then
# MAGIC    - use agg() or selectExpr() on the resulting GroupedData object. 
# MAGIC
# MAGIC Note that selectExpr() is generally called after the groupBy to select the final columns, which often results in the same effect as using agg()
# MAGIC
# MAGIC A common approach to applying aggregation expressions using SQL syntax within the DataFrame API is to use the agg() method on the grouped data:
# MAGIC
# MAGIC ##### This is teh standard way in pyspark to groupe aggregations. THIS PIECE CONTAINS A field("department") and aggregations(sum and avg) but the field is grouped first

# COMMAND ----------


"""from pyspark.sql import functions as F
df.groupBy("department")
  .agg(F.sum("salary").alias("total_salary"),
       F.avg("salary").alias("avg_salary")
      )
"""


# COMMAND ----------

# MAGIC %md
# MAGIC ##### using selectExpr() after groupBy()
# MAGIC

# COMMAND ----------

"""
from pyspark.sql import functions as F

# Sample Data
data = [("Sales", 100), ("Sales", 200), ("Marketing", 50)]
df = spark.createDataFrame(data, ["department", "revenue"])

# Aggregation followed by selectExpr
result = df.groupBy("department") \
           .agg(F.sum("revenue").alias("total")) \
           .selectExpr("department", "total", "total * 1.1 as revenue_with_tax")

result.show()

"""

# COMMAND ----------

# MAGIC %md
# MAGIC ##### If you prefer to use the groupBy() INSIDE THE selectExpr(), you would typically register the DataFrame as a temporary view first, and then run a SQL query:
# MAGIC

# COMMAND ----------

# Using selectExpr() with groupBy is less direct than using agg()
# A common pattern is to register a temp view and use spark.sql()
#df.createOrReplaceTempView("products_view")
#spark.sql("SELECT Product, AVG(Price) AS avg_price, SUM(Quantity) AS total_quantity FROM products_view GROUP BY Product").show()


# COMMAND ----------

# MAGIC %md
# MAGIC -------------------------------------------------------
# MAGIC
# MAGIC
# MAGIC #### 2. Global Aggregation (on the entire DataFrame) 
# MAGIC You can perform aggregations on the entire DataFrame without explicit grouping. This is a shorthand for df.groupBy().agg()

# COMMAND ----------

"""
from pyspark.sql import SparkSession

# Create a SparkSession (if not already created)
spark = SparkSession.builder.appName("selectExprAgg").getOrCreate()

# Sample DataFrame
data = [("Laptop", 1500, 10), 
        ("Mouse", 50, 200),
        ("Laptop", 1200, 5),
        ("Keyboard", 100, 50)]
columns = ["Product", "Price", "Quantity"]
df = spark.createDataFrame(data, columns)
df.show()


# Global aggregation using selectExpr()
df.selectExpr(
    "avg(Price) AS avg_price",
    "sum(Quantity) AS total_quantity",
    "count(Product) AS total_products"
).show()
"""

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### PAY ATTENTION!! This piece of teh above code DOES NOT apply aggregations to a GROUPBY  any DIMENSION/FIELD, IT ONLY DOES AGGREGATIONS
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC -----------------------------------------------------------
# MAGIC #### 4  where/filter/having
# MAGIC -----------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ##### In PySpark
# MAGIC ###### where() and filter() 
# MAGIC Both are used to filter rows based on a given condition, similar to the SQL WHERE clause. They are interchangeable and can accept boolean expressions or SQL-style string conditions
# MAGIC
# MAGIC ###### having() 
# MAGIC In PySpark, 'having' IS NOT USED. You have two options instead:
# MAGIC   -     Applying a filter() or where() condition after the groupBy() and aggregation steps have been performed
# MAGIC   -     Using SQL queries directly 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SQL Query
# MAGIC
# MAGIC
# MAGIC .
# MAGIC
# MAGIC #####Pyspark
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC -----------------------------------------------------
# MAGIC #### 5 regexp_replace()  
# MAGIC -----------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ##### To replace strings in PySpark, 
# MAGIC the most efficient methods use built-in functions like:
# MAGIC
# MAGIC - regexp_replace for pattern matching or 
# MAGIC - the DataFrame.replace method for direct value substitution.
# MAGIC
# MAGIC
# MAGIC ##### 1. regexp_replace (Pattern-based)
# MAGIC Used for complex string manipulation within a column using regular expressions. It replaces every substring that matches a specific pattern
# MAGIC - Syntax: regexp_replace(column, pattern, replacement)
# MAGIC - Key Feature: Supports regex characters like \d (digits), ^ (start), and $ (end).
# MAGIC

# COMMAND ----------

"""
from pyspark.sql.functions import regexp_replace
# Replaces all digits with 'X' in the "phone" column
df.withColumn("masked_phone", regexp_replace("phone", r"\d", "X"))

"""

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. replace (Value-based)
# MAGIC There are two distinct "replace" methods in PySpark:
# MAGIC
# MAGIC Method 	---------------Level ----------	Description
# MAGIC - df.replace() ----	 DataFrame	--- Replaces exact values across the whole DataFrame or a subset of columns.
# MAGIC - F.replace()	------ Column	   ------ Replaces exact substrings within a string column (Introduced in Spark 3.5.0).
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###### DataFrame replace Example:

# COMMAND ----------

"""
# Replaces "Male" with "M" in the "Gender" column
df.replace("Male", "M", subset=["Gender"])

# Using a dictionary for multiple replacements at once
df.replace({"John": "Jonathan", "Jane": "Janet"}, subset=["Name"])

"""

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Column-Level replace Example:

# COMMAND ----------

"""
from pyspark.sql import functions as F
# Replaces the literal substring "lane" with "ln"
df.withColumn("address", F.replace("address", "lane", "ln"))

"""

# COMMAND ----------

# MAGIC %md
# MAGIC Summary of Differences
# MAGIC
# MAGIC -----------------------------------------
# MAGIC https://www.google.com/search?q=regexp_replace+and+replace+pyspark&client=firefox-b-d&hs=1EyU&sca_esv=91aac8a642e70fa5&biw=1680&bih=739&sxsrf=ANbL-n5PaxFdRNdA4xxVEC9Sgxk7KUL7nw%3A1775176464643&ei=EAvPad-AJ4yrur8P7OKj-Qs&oq=regexp_replace+and+replace+pysp&gs_lp=Egxnd3Mtd2l6LXNlcnAiH3JlZ2V4cF9yZXBsYWNlIGFuZCByZXBsYWNlIHB5c3AqAggAMgUQIRigATIFECEYoAFIuqUBUABYjJEBcAJ4AZABAJgB3gGgAZYTqgEGMS4xNy4xuAEDyAEA-AEBmAIVoAL_FcICCxAuGIAEGLEDGIMBwgIIEC4YgAQYsQPCAggQABiABBixA8ICBRAAGIAEwgIaEC4YgAQYsQMYgwEYlwUY3AQY3gQY4ATYAQHCAgQQIxgnwgIMECMYgAQYExgnGIoFwgIKEAAYgAQYQxiKBcICFhAuGIAEGLEDGNEDGEMYgwEYxwEYigXCAgoQIxiABBgnGIoFwgIKEC4YgAQYQxiKBcICDRAAGIAEGLEDGEMYigXCAgoQABiABBgUGIcCwgIIEAAYgAQYywHCAgcQABiABBgKwgIGEAAYDRgewgIIEAAYChgNGB7CAgQQABgewgIIEAAYFhgKGB7CAgYQABgWGB7CAggQABiABBiiBMICBRAAGO8FmAMAugYGCAEQARgUkgcGMi4xNy4yoAezc7IHBjAuMTcuMrgH8RXCBwswLjMuOC43LjEuMsgHjgKACAA&sclient=gws-wiz-serp
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC -----------------------------------------------------------
# MAGIC #### 6  cast(), try_cast()
# MAGIC -----------------------------------------------------------
# MAGIC
# MAGIC In PySpark,
# MAGIC cast() and try_cast() are used to convert a column from one data type to another, but they handle conversion failures differently, especially when ANSI mode is enabled
# MAGIC
# MAGIC Key Differences
# MAGIC Feature 	
# MAGIC
# MAGIC cast()	
# MAGIC - Success: Returns the converted value.
# MAGIC - Failure (Non-ANSI):	Usually returns null.	
# MAGIC - Failure (ANSI Mode):	Throws an error (e.g., SparkArithmeticException).
# MAGIC
# MAGIC try_cast()
# MAGIC - Success:	Returns the converted value.	
# MAGIC - Failure (Non-ANSI):	Returns null.
# MAGIC - Failure (ANSI Mode):	Returns null instead of failing.
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

"""
from pyspark.sql.functions import col

# Standard cast (may fail in ANSI mode if data is invalid)
df.withColumn("age", col("age_str").cast("int"))

# Try cast (returns NULL if conversion is impossible)
# Available in PySpark 4.0+
df.withColumn("age", col("age_str").try_cast("int"))

df.selectExpr("try_cast(age_str AS int) as age")


"""

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ![image_1774641851028.png](./image_1774641851028.png "image_1774641851028.png")
# MAGIC
# MAGIC -----------------------------------------------------------
# MAGIC ####     7  casting to date fails
# MAGIC -----------------------------------------------------------
# MAGIC
# MAGIC ##### 1 string field containing date values
# MAGIC A string field containing date values often cannot be cast directly in PySpark using a simple.cast("date") because PySpark expects a default, ISO-compliant date format of yyyy-MM-dd. 
# MAGIC
# MAGIC If your input string is in any other format (e.g., MM/dd/yyyy, dd-MM-yyyy, or yyyyMMdd), the direct cast will likely result in null values or a SparkDateTimeException error.
# MAGIC
# MAGIC #####2 Carefull with columns cotaining STRING 'null'/'Null' values
# MAGIC Columns containing a string 'null'/'Null' values  INSTEAD of containing actual nulls or no values can cause SEVERAL ERRORS

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774890256343.png](./image_1774890256343.png "image_1774890256343.png")
# MAGIC
# MAGIC ![image_1774890573091.png](./image_1774890573091.png "image_1774890573091.png")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ![image_1774641851028.png](./image_1774641851028.png "image_1774641851028.png")
# MAGIC #### 6  tbd

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ![image_1774641851028.png](./image_1774641851028.png "image_1774641851028.png")
# MAGIC #### 6  tbd

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ![image_1774641851028.png](./image_1774641851028.png "image_1774641851028.png")
# MAGIC #### 6  tbd

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ![image_1774641851028.png](./image_1774641851028.png "image_1774641851028.png")
# MAGIC #### 6  tbd

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ![image_1774641851028.png](./image_1774641851028.png "image_1774641851028.png")
# MAGIC #### 6  tbd

# COMMAND ----------

