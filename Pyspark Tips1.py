# Databricks notebook source
# MAGIC %md
# MAGIC ##Agenda
# MAGIC
# MAGIC ### 3 Pyspark Tips  
# MAGIC      1-   Methods for dataframes(read, format, etc, )
# MAGIC      2-   Select() vs selectExpr() vs expr()
# MAGIC      3-   Grouping vs agg()
# MAGIC      4-   where/filter/having
# MAGIC      5-   regexp_replace(), replace()
# MAGIC      6-   cast(), try_cast()
# MAGIC      7-   Dates (datediff, interval)
# MAGIC      8-   .agg(max("column")).first()[0] # max_value
# MAGIC      9-   join(): Handling duplicate/ambiguous Columns / alias
# MAGIC      9.1- Lateral join, left_semi, left_anti, 
# MAGIC      10-  CTE
# MAGIC      11-  withColumn/withColumns calling a newly created column in the same transformation is NOT ALLOWED
# MAGIC      12-  Renaming a column. Use withCoulumnRenamed/withCoulumnsRenamed
# MAGIC      13-  Remove/Drop a column(s) 
# MAGIC      14-  Use AI to parse and extract the required information from unstructured data
# MAGIC      15-  Nulls s
# MAGIC      16-  Complex Data Types
# MAGIC      17-  Window Functions
# MAGIC      18-  LAG
# MAGIC      19-  Temporary View
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
# MAGIC #### 2 select() vs selectExpr()
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
# MAGIC                       operates on a single SQL expression and returns a Column object for use within other DataFrame methods, 
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
# MAGIC                     - Does the sam e as 'col(col)' but 'col(col)' is more preferred. 
# MAGIC #####  lit(col)           
# MAGIC                     - Creates a Column of literal value.
# MAGIC #####  try_cast(dataType) 
# MAGIC                     - A special version of cast that performs the same operation, but returns a NULL value instead of raising 
# MAGIC                       an error if the invoke method throws exception.
# MAGIC """
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

"""

The core difference between PySpark's
The core difference is that 
1)    select() accepts column objects or raw column name strings. Uses the DataFrame API's column objects and functions
2)    selectExpr() accepts SQL-style expression strings
3)    expr() is a function that parses a SQL expression string into a Column object. 
             Useful when you want to apply a SQL expression to a DataFrame column or perform a SQL-like operation on a DataFrame.

          Example: # 1. Combining Column objects and SQL strings within a standard select()
                      df.select(col("name"), expr("age + 1 as next_year_age"))

          Example: # 2. Using it inside a filter condition
                      df.filter(expr("age > 18 AND status = 'Active'"))

          Example: # 3. Using expr() with sum, max, etc.
             
                        Calculates the total sum of a column
                        df.agg(expr("sum(sales)")).show()

                        Evaluates conditional math inside the sum
                        df.select(expr("sum(price * quantity)")).show() 

          WARNING: col() cannot be nested inside expr() because :
                    expr() expects a raw SQL STRING but returns a Column object, you can use it interchangeably with col() 
                    col() returns a Column object NOT A STRING.
                                  
                    # ❌ THIS WILL FAIL
                      df.select(expr(col("age") + 1))


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
<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< .col vs. .column >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
col and column are functionally identical. 
Both are functions within the pyspark.sql.functions module that return a Column object based on a given string name.

Convention: 
col is much more commonly used in the PySpark community because it is shorter and more concise

col(col)           - Se utiliza para hacer referencia a una columna de un DataFrame por su nombre, devolviendo un objeto de tipo Column. 
                     Es fundamental para transformaciones, filtros y ordenaciones al convertir cadenas de texto en expresiones de columna. 
                     col("nombre_columna").
column(col)        - Does the sam e as 'col(col)' but 'col(col)' is more preferred. 


lit(col)           - Creates a Column of literal value.
try_cast(dataType) - A special version of cast that performs the same operation, but returns a NULL value instead of raising an error if 
                     the invoke method throws exception.
"""

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

In expr(), parses a SQL expression string into a Column object, allowing you to write raw SQL logic directly inside DataFrame transformations. 




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
# MAGIC
# MAGIC ##### ----> What is GroupBy?
# MAGIC
# MAGIC PySpark groupBy( ) is used to split the data into groups based on one or more columns, which can then be aggregated or transformed independently.
# MAGIC
# MAGIC
# MAGIC Group data
# MAGIC ###### 1- grouped = df.groupBy("department")
# MAGIC
# MAGIC Once data is grouped, you can  apply aggregations .count() or .sum() to obtain a new DataFrame.
# MAGIC
# MAGIC ###### 2- grouped.count().show()
# MAGIC
# MAGIC ##### ----> Parameters and return values
# MAGIC
# MAGIC The only parameter for the method is *cols which accepts:
# MAGIC - column names 
# MAGIC - column expressions
# MAGIC - column ordinals (int) 
# MAGIC - list of columns.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### DatFrame example data

# COMMAND ----------

from pyspark.sql import Row

data = [
    Row(department="Sales", employee="Alice", salary=5000),
    Row(department="Sales", employee="Bob", salary=4800),
    Row(department="HR", employee="Carol", salary=4000),
    Row(department="HR", employee="David", salary=3900),
    Row(department="IT", employee="Eve", salary=6000)
]

df = spark.createDataFrame(data)
df.show()

# COMMAND ----------

#grouping by column NAME like above
df.groupBy("department")

#grouping by column EXPRESSION
df.groupBy(df.department)

#grouping by column ORDINAL
df.groupBy(1)

#grouping by LIST of COLUMNS, you can mix the methods!
df.groupBy(["department", 2])


# COMMAND ----------

# MAGIC %md
# MAGIC ##### ----> GroupBy on single and multiple columns

# COMMAND ----------

#grouping by SINGLE COLUMN
df.groupBy("department").sum(“salary”).show()

#grouping by MULTIPLE COLUMNS
df.groupBy(["department", 2]).sum(“salary”).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ----> Multiple aggregations with agg()
# MAGIC
# MAGIC We do not need to aggregate on the same column with agg(), you can define a different column for each aggregation function.

# COMMAND ----------

# After already starting your session
from pyspark.sql.functions import count, sum, avg, max, min

df.groupBy("department").agg( count("employee").alias("employee_count"), 
                              avg("salary").alias("avg_salary"),
                              max("salary").alias("max_salary")
                            ).show()
  

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ----> Multiple aggregations with agg( expr() )
# MAGIC
# MAGIC You can use aggregation and expr() function but apply these changes:
# MAGIC
# MAGIC expr() expects a string argument (a SQL expression), but in teh code above the functions count(), avg(), and max() return Column objects, not strings.
# MAGIC
# MAGIC The fix is simple: expr() should wrap the SQL expression as a string, not wrap Column objects.

# COMMAND ----------

# After already starting your session
from pyspark.sql.functions import count, sum, avg, max, min, col, expr

df.groupBy("department").agg( expr("count(employee) as employee_count"), 
                              expr("avg(salary) as avg_salary"),
                              expr("max(salary) as max_salary")
                              
                            ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Advanced aggregations 
# MAGIC https://www.datacamp.com/tutorial/pyspark-groupby
# MAGIC ###### ---> Pivoting
# MAGIC ###### ---> Rollups and cubes
# MAGIC ###### ---> Grouping sets
# MAGIC ###### ---> Custom aggregation functions
# MAGIC ###### ---> PySpark groupBy Performance Optimization Strategies
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ----> Filtering Aggregated Data
# MAGIC
# MAGIC In PySpark, you can filter groups based on aggregate metrics post-grouping using the filter( ) or where( ) methods. 
# MAGIC -   You need to provide a condition in either Python or SQL expressions. 
# MAGIC -   You can filter BEFORE or AFTER the aggregation. 
# MAGIC     -   Filtering before will impact the aggregation by limiting what data gets aggregated and can improve performance. 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ----> PySpark SQL GROUP BY Query using  a temporary view 
# MAGIC
# MAGIC If you are more comfortable writing SQL, use the SQL API to write statements.
# MAGIC
# MAGIC ###### ---> First create a temporary view 
# MAGIC First step is to create a temporary view using the createOrReplaceTempView() method of the DataFrame
# MAGIC Then you can use spark.sql() to write your statement.

# COMMAND ----------

# Create a temporary view using the DataFrame
df.createOrReplaceTempView("employees")

# Write a SQL-like statement
sql_query = """
    SELECT department, AVG(salary) AS avg_salary
    FROM employees
    GROUP BY department
"""

result_df =  spark.sql(sql_query)
result_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ---------------------------------------------------------------
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


"""from pyspark.sql.functions import col, sum, avg
df.groupBy("department")
  .agg(sum("salary").alias("total_salary"),
       avg("salary").alias("avg_salary")
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
# MAGIC ##### 2. Global Aggregation (on the entire DataFrame) 
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
# MAGIC ### Example: Multiple filters

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SQL Query
# MAGIC
# MAGIC ###### What San Francisco neighborhoods in in the zip codes 94102 and 94103
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT City, Neighborhood, Zipcode
# MAGIC FROM dev.spark_db.sf_fire_calls
# MAGIC WHERE City = 'SF' and Zipcode in (94102 , 94103);

# COMMAND ----------

# MAGIC %md
# MAGIC #####Pyspark
# MAGIC
# MAGIC ###### isin(), &

# COMMAND ----------

from pyspark.sql.window    import Window
from pyspark.sql.functions import rank, col, count, sum, expr, desc

result_df = ( raw_fire_df.where( (raw_fire_df["Zipcode"].isin([94102 , 94103])) & (raw_fire_df["City"]=='SF') )
                         .select("City", "Neighborhood", "Zipcode")
            )

result_df.display()

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
# MAGIC ####     7  Dates(datediff, interval)
# MAGIC
# MAGIC - Folder: CH06-Working with Data Types
# MAGIC - Notebook: 04-Working with dates
# MAGIC
# MAGIC ######  Convert String to date        
# MAGIC ######  Add, Subtract days and months to date
# MAGIC ######  Current date, date difference, and interval
# MAGIC ######  Format date 
# MAGIC ######  casting to date fails
# MAGIC
# MAGIC -----------------------------------------------------------
# MAGIC
# MAGIC ##### 1 string field containing date values
# MAGIC A string field containing date values often cannot be cast directly in PySpark using a simple.cast("date") because PySpark expects a default, ISO-compliant date format of yyyy-MM-dd. 
# MAGIC
# MAGIC If your input string is in any other format (e.g., MM/dd/yyyy, dd-MM-yyyy, or yyyyMMdd), the direct cast will likely result in null values or a SparkDateTimeException error.
# MAGIC
# MAGIC #####2 Carefull with columns cotaining STRING 'null'/'Null' values
# MAGIC Columns containing a string 'null'/'Null' values  INSTEAD of containing actual nulls or no values can cause SEVERAL ERRORS
# MAGIC
# MAGIC #####3 expr('try_to_date') 
# MAGIC try_to_date' is a string function, so you do not need to import it, that tries to cast into date, if it fails to do so it will return a Null value.
# MAGIC
# MAGIC #####4 date_format 
# MAGIC when date_format is used it will return  a STRING not a date.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774890256343.png](./image_1774890256343.png "image_1774890256343.png")
# MAGIC
# MAGIC ![image_1774890573091.png](./image_1774890573091.png "image_1774890573091.png")

# COMMAND ----------

# MAGIC %md
# MAGIC #### datediff()
# MAGIC
# MAGIC The pyspark.sql.functions import datediff() function in PySpark calculates the number of days between two dates by evaluating end_date - start_date
# MAGIC
# MAGIC Parameters:
# MAGIC 1)      end: The end date column (Minuend).
# MAGIC 2)      start: The start date column (Subtrahend).
# MAGIC
# MAGIC 3)      If end is after start, the result is positive.
# MAGIC 4)      If end is before start, the result is negative.
# MAGIC
# MAGIC

# COMMAND ----------

windowSpec = ( Window.partitionBy().orderBy('ORDER_DATE') 
             )

cust_df = ( raw_cust_df.select("customer_name", "ORDER_ID", "ORDER_DATE")
                       .withColumns({ "PREV_PURCHASE" : lag("ORDER_DATE").over(windowSpec)
                                   })
                       .filter(date_diff(col('ORDER_DATE'), col('PREV_PURCHASE') ) == 1)

          ) 
cust_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### interval
# MAGIC interval represents a fixed duration of time (like 5 days, 3 months, or 2 hours) used primarily for date and timestamp arithmetic.
# MAGIC
# MAGIC -----------------------------------------------
# MAGIC #####1. Using SQL Expressions (Most Common)
# MAGIC You can inject intervals directly into your PySpark column logic using the expr() function with standard SQL syntax.

# COMMAND ----------

import pyspark.sql.functions as F

# Adding 5 days to a date column
df.withColumn("new_date", F.expr("current_date + INTERVAL 5 DAYS"))

# Subtracting 3 months and 2 hours
df.withColumn("past_time", F.expr("current_timestamp - INTERVAL '3-2' YEAR TO MONTH"))


# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------------
# MAGIC #####2. Built-in PySpark Functions
# MAGIC Starting in Spark 3.x, you can build intervals dynamically from column values using built-in constructor functions:
# MAGIC 1)      make_interval(): Constructs an interval out of years, months, weeks, days, hours, mins, and secs.
# MAGIC 2)      make_dt_interval(): Constructs a day-time specific interval.
# MAGIC 3)      make_ym_interval(): Constructs a year-month specific interval.
# MAGIC

# COMMAND ----------

# Constructing an interval dynamically from integer columns
df.withColumn("duration", F.make_interval(df.years, df.months, F.lit(0), df.days))


# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------------
# MAGIC #####3. Native Interval Data Types
# MAGIC Modern PySpark breaks intervals down into two highly optimized data types instead of the legacy calendar type:

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, DayTimeIntervalType

# Defining a schema with an explicit interval column
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("duration_window", DayTimeIntervalType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ------------------------------------------------------------------------------
# MAGIC #### 8  .agg(max("column")).first()[0] # max_value
# MAGIC -------------------------------------------------------------------------------
# MAGIC
# MAGIC In PySpark, the sequence agg(...).first()[0] is a common pattern used to extract a single scalar value from an aggregated DataFrame.
# MAGIC
# MAGIC max_salary = raw_emp_df.agg(max("salary")).first()[0] # max_salary

# COMMAND ----------

"""
Breakdown of the Sequence:

    raw_emp_df: Es el nombre del DataFrame que contiene los datos de los empleados.
    .agg(max("salary")):
        agg es la función de agregación.
        max_("salary") calcula el valor máximo de la columna llamada "salary".
        Nota: El resultado de este paso sigue siendo un DataFrame con una sola fila y una sola columna.
    .first()[0]:
        Esta es una "acción" de Spark que toma la primera fila del DataFrame resultante y la devuelve como un objeto tipo Row de Spark
        [0]: Accede al primer elemento de esa fila (el valor numérico del salario máximo). 
        Sin esto, tendrías un objeto Row(max(salary)=5000), pero con el [0] obtienes simplemente el 5000.
"""

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ------------------------------------------------------------------
# MAGIC #### 9  join(): Handling duplicate/ambiguous columns / alias
# MAGIC ------------------------------------------------------------------
# MAGIC
# MAGIC 1. Joining tables can be done in different ways:
# MAGIC -       Creating a datframe BEFORE tables are LOADED by reading files from Volumens using the FOOL format. 
# MAGIC         spark.read.format.....
# MAGIC
# MAGIC -       Creating a dataframe AFTER the tables were LOADED with data from a file. CALLING THE ALREADY LOADED TABLE DIRECTLY
# MAGIC         spark.table.....
# MAGIC
# MAGIC 3. Handling Duplicate Columns When using a boolean expression (like df1.id == df2.id), both columns will appear in the result, which can cause "ambiguous column" errors later.  
# MAGIC
# MAGIC ###### Fix:
# MAGIC
# MAGIC -       Use alias: A better approach is to assign aliases to the dataframes, and then reference the output columns from the join operation using these aliases:
# MAGIC         https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html
# MAGIC -       Use .drop(df2.id) after the join 
# MAGIC -       use the string format on="id" if the names are identical, as Spark will automatically merge them into one column.
# MAGIC
# MAGIC
# MAGIC ##### Example: Total revenue and number of ordes per region
# MAGIC
# MAGIC Find it in this path:
# MAGIC
# MAGIC spark_programmingHBL / HBL_Practice_Transformations-Query the data / 1 SQL Interviews-customer_product
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Join example 1:   Creating a datframe BEFORE tables are LOADED by reading files from Volumens using the FOOL format. 
# MAGIC   spark.read.format.....
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

"""
from pyspark.sql.functions import expr, col, count, max, sum

result_df =  ( raw_cust_df.alias('C').join(raw_ord_df.alias('O'), col("C.ORDER_ID") == col("O.ORDER_ID"), how="inner")
                                     .groupBy("region")
                                     .agg( sum(expr("quantity * price")).alias("total_revenue"), count(col("C.ORDER_ID")).alias("order_count"))
                                     .orderBy(col("total_revenue").desc() )

             )

result_df.display()
"""


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC FGind it in t6his path: spark_programming /CH07-Spark Joins / 02- Inner Joins
# MAGIC
# MAGIC ###### Join ecxample 2: Creating a dataframe AFTER the tables were LOADED with data from a file. CALLING THE ALREADY LOADED TABLE DIRECTLY
# MAGIC   spark.table.....
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import expr, col

# Here we are NOT using the spark.read. FOOL format, instead we call the tables, which now contain data, directly spark.table...
members_df = spark.table("dev.spark_db.members").alias("m") 
bookings_df = spark.table("dev.spark_db.bookings").alias("b")

#join_expr = expr("m.menber_id == b.member_id")
join_expr = col("m.member_id") == col("b.member_id") # Here we save the join condition/expression in a variable

reports_df = (
    members_df.join(bookings_df, join_expr, "inner") # Here we use teh join condition/expression variable
        .filter("m.last_name == 'Smith' and b.slots > 5")
        .select("m.member_id", "m.first_name", "m.last_name", "b.facility_id", "b.slots", "b.start_time")
        .orderBy("m.first_name", col("b.slots").desc())
)

reports_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Join example 3: 

# COMMAND ----------

from pyspark.sql.functions import col, expr

bookings_df = spark.table("dev.spark_db.bookings").alias("b")
members_df = spark.table("dev.spark_db.members").alias("m")
facilities_df = spark.table("dev.spark_db.facilities").alias("f")

report_df = (bookings_df.join(members_df, expr("b.member_id == m.member_id"), "inner")
                        .join(facilities_df, col("b.facility_id") == col("f.facility_id"), "inner")
                        .filter("m.last_name == 'Smith' and b.slots > 5")
                        .selectExpr("m.member_id", "m.first_name", "m.last_name", "f.facility_name", "b.slots", "b.slots * f.member_cost as booking_amount", "b.start_time")
                        .orderBy(col("m.first_name").asc(),col("booking_amount").desc())
)

report_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---------------------------------------------------------
# MAGIC ###9.1 Lateral join, left_semi, left_anti, 
# MAGIC ---------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Lateral join allows to query right dataframe for each row of the left dataframe.
# MAGIC
# MAGIC A lateral join (also called a correlated join) allows each row from a left DataFrame to be used as input for a subquery or derived table on the right side, meaning the right side can reference columns from the left side
# MAGIC
# MAGIC
# MAGIC Lateral joins are especially useful when:
# MAGIC 1. You need per-parent Top-N child rows
# MAGIC         e.g. Say the parent record is the left df and you want onw or two top records from the right df
# MAGIC 2. You want to invoke TVFs(Table Value Functions) with arguments derived from each row
# MAGIC   
# MAGIC ###### notebook 04- Later Joins  
# MAGIC ---------------------------------------------------------------

# COMMAND ----------

spark.version

from pyspark.sql.functions import col, expr

data = [ ("inner", "INNER JOIN", "Keeps only rows with matching keys in both DataFrames. "), 
        ("left, leftouter, left_outer", "LEFT OUTER JOIN ", "Keeps all rows from the left DataFrame, and matched columns from the right. Missing matches get null. "),
        ("right, rightouter, right_outer", "RIGHT OUTER JOIN", " Keeps all rows from the right DataFrame, and matched columns from the left. Missing matches get null. "),
        ("outer, full, fullouter, full_outer", "FULL OUTER JOIN", "Keeps all records from both sides. Fills unmatched rows with null. "),
        ("left_semi, semi", "None (Often EXISTS)", "Returns only left DataFrame columns for rows that have a matching key on the right. "),
        ("left_anti, anti", "None (Often NOT EXISTS)", "Returns only left DataFrame columns for rows that do not have a match on the right. "),
        ("cross", "CROSS JOIN", "Generates a Cartesian product (pairs every row of the left table with every row of the right table")
       ]

# Create DataFrame
df = spark.createDataFrame(data, ["PySpark_String", "LiteralsEquivalent", "SQL_ClauseDescription"])
df2 = df.selectExpr("PySpark_String", "LiteralsEquivalent", "SQL_ClauseDescription")
df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC --------------------------------------------------------------
# MAGIC #### 10 CTE
# MAGIC --------------------------------------------------------------
# MAGIC
# MAGIC How to Use CTEs in PySparkTo use CTEs in PySpark, you must register a DataFrame as a temporary view, then use spark.sql() to execute the query.
# MAGIC
# MAGIC ##### If your SQL Query contains a CTE is better to use a temporaryView to replicate that code in Pyspark
# MAGIC
# MAGIC #### Example
# MAGIC

# COMMAND ----------

# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame.
# Basically the "raw_cust_df" dataframe was built, using the FOOL format, from the customers.csv file 
raw_cust_df.createOrReplaceTempView('customers') # We give the view  a name 'customers'
#raw_ord_df.createOrReplaceTempView('orders')   # We give the view  a name 'orders'

# Run the SQL query using spark.sql()
sql_query="""
WITH CTE AS (  SELECT  CUSTOMERID, ORDER_ID, SUM(QUANTITY * PRICE) AS ORDER_VALUE
               FROM   customers    AS C 
               GROUP BY CUSTOMERID, ORDER_ID
            ) 

SELECT CUSTOMERID
      ,AVG(ORDER_VALUE)
      ,(SELECT AVG(ORDER_VALUE) FROM CTE) AS AVG_ORDER_VALUE
FROM CTE
WHERE ORDER_VALUE > (SELECT AVG(ORDER_VALUE) FROM CTE)
GROUP BY  CUSTOMERID
;
"""

result_df =  spark.sql(sql_query)
result_df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ------------------------------------------------------
# MAGIC #### 11  WithColumn/WithColumns Calling a newly created column in the same transformation
# MAGIC ------------------------------------------------------

# COMMAND ----------

"""from pyspark.sql.functions import expr, col, count, min, max, sum, avg, lag
#from pyspark.sql.window import Window

# Although two changes are happening is considered ONE tramsformation "withColumns" cause even though two transformations are happening both are INSIDE the withcolumns transformation
result_df = ( raw_cust_df.withColumns({ "Newprice": col("price") * 1.1,  # Creating a new column 'Newprice'
                                        "Price": expr("case when order_date < '2022-01-01' then price else Newprice end") # Calling the newly cretaed column 'Newprice' inthe same transformations yields an error
                                     })                  
            ) 

result_df.display()
"""


# COMMAND ----------

# MAGIC %md
# MAGIC ---------------------------------------------------------------
# MAGIC ### Error
# MAGIC [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `Newprice` cannot be resolved.

# COMMAND ----------

# MAGIC %md
# MAGIC ---------------------------------------------------
# MAGIC #### Use two separate transformations INSTEAD
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import expr, col, count, min, max, sum, avg, lag
#from pyspark.sql.window import Window

result_df = ( raw_cust_df.withColumn( "Newprice", col("price") * 1.1)
                         .withColumn( "Price", expr("case when order_date < '2022-01-01' then price else Newprice end") ) # Calling the newly cretaed column 'Newprice' inthe separate transformations is fine                              
            )

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC -----------------------------------------------------------
# MAGIC #### 12- Renaming a column. Use withCoulumnRenamed/withCoulumnsRenamed
# MAGIC -----------------------------------------------------------
# MAGIC      

# COMMAND ----------

from pyspark.sql.functions import expr, col, count, min, max, sum, avg, lag
#from pyspark.sql.window import Window

result_df = ( raw_cust_df.withColumnRenamed( "productid", "product" )    
            ) 

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------------------------
# MAGIC #### 13  Remove/Drop a column(s)
# MAGIC -----------------------------------------------------------

# COMMAND ----------

from pyspark.sql.functions import expr, col, count, min, max, sum, avg, lag
#from pyspark.sql.window import Window

result_df = ( raw_cust_df.drop( "productid") # to drop multiple columnns just add a ',' a comma between the column names    
            ) 

result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------------------------
# MAGIC ####  14- Use AI to parse and extract the required information from unstructured data
# MAGIC -----------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC See notebook: 05-Transforming Unstructured data

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------------------------
# MAGIC #### 15 Nulls
# MAGIC
# MAGIC Check the:
# MAGIC
# MAGIC Nulls Folder: CH06-Working with Data Types
# MAGIC
# MAGIC ###### Notebook: 01-Working with nulls
# MAGIC
# MAGIC
# MAGIC ##### Examples:
# MAGIC
# MAGIC ###### Filter rows where a specific column is null
# MAGIC 1)      df.filter( col("column_name").isNull( ) ).show()
# MAGIC
# MAGIC ###### Filter rows where ANY of the specified columns are null
# MAGIC 2)      df.filter( col("col1").isNull( ) | col("col2").isNull( ) ).show()
# MAGIC
# MAGIC
# MAGIC ###### With a dataframe field
# MAGIC 3)      df.filter(df.departmentid.isNull())
# MAGIC
# MAGIC
# MAGIC
# MAGIC ###### Drop or Fill Null Values
# MAGIC Once you find the nulls, you can handle them using the DataFrame.na functions:Drop rows: 
# MAGIC
# MAGIC 1)      df.na.drop() drops any row containing a null value. 
# MAGIC 2)      df.na.fill("Unknown") Fill values: replaces null strings with "Unknown".
# MAGIC
# MAGIC -----------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------------------------
# MAGIC #### 16  Complex Data Types
# MAGIC -----------------------------------------------------------
# MAGIC - Folder: CH06-Working with Data Types
# MAGIC - Notebook: 06-Working with complex data types
# MAGIC
# MAGIC Complex Data Types in Spark
# MAGIC
# MAGIC     Struct
# MAGIC     Array
# MAGIC     Map
# MAGIC     VARIANT
# MAGIC
# MAGIC In PySpark, Array, Struct, and Map are traditional complex data types used to handle nested data, while VARIANT is a newer, high-performance type introduced in Spark 4.0 (and widely used in Databricks) for semi-structured data like JSON.
# MAGIC
# MAGIC     1.ArrayTypeAn ArrayType stores a sequence of elements, all of which must have the same data type.
# MAGIC     -   Best for: Lists of similar items (e.g., tags, transaction IDs).
# MAGIC     -   Key Operations:explode(): Transforms each element of an array into a new row.array_contains(): Checks if a specific value exists in the array.size(): Returns the number of elements.
# MAGIC     
# MAGIC     2.StructTypeA StructType is a collection of StructFields, where each field has a name and its own specific data type.
# MAGIC     -   Best for: Grouping related but different types of data (e.g., an address struct containing street (string) and zip (integer)).
# MAGIC     -   Key Operations:Access fields using dot notation (e.g., df.select("address.city")).Use inline() to flatten an array of structs into multiple columns.
# MAGIC     
# MAGIC     3.MapTypeA MapType stores key-value pairs. All keys must be the same type, and all values must be the same type.
# MAGIC     -   Best for: Flexible schemas where you don't know all the keys in advance (e.g., user-defined attributes).
# MAGIC     -   Key Operations:create_map(): Creates a map from existing columns.map_keys() / map_values(): Extracts all keys or values as an array.
# MAGIC     
# MAGIC     4.VARIANT (New in Spark 4.0)The VariantType is a specialized binary format designed to store semi-structured data (like JSON) more efficiently than a string or a complex Map/Struct.
# MAGIC     -   Best for: High-performance ingestion of raw JSON where the schema is unknown or constantly changing.
# MAGIC     -   Key Operations:to_variant_object(): Converts nested inputs (arrays/maps/structs) into a Variant.variant_get(): Extracts specific fields using path expressions (e.g., $.data).schema_of_variant():
# MAGIC         Infers the SQL schema from a variant column.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1778612677944.png](./image_1778612677944.png "image_1778612677944.png")

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------------------------
# MAGIC #### 17 Window Functions
# MAGIC -----------------------------------------------------------
# MAGIC It is not allowed to use a window function inside an aggregate function. Please use the inner window function in a sub-query.
# MAGIC
# MAGIC ##### Core Anatomy of a Window Function
# MAGIC Every window operation in PySpark requires building a WindowSpec using the pyspark.sql.Window class. 
# MAGIC
# MAGIC It consists of three structural pillars:
# MAGIC 1)      partitionBy(): Groups rows into subsets based on one or more columns (similar to GROUP BY in SQL).
# MAGIC 2)      orderBy(): Sorts the rows within each partition to define a sequence.
# MAGIC 3)      Frame Specification (rowsBetween or rangeBetween): Defines the boundaries of the calculation relative to the 
# MAGIC         current row (e.g., row-based offsets or range-based value boundaries).
# MAGIC
# MAGIC
# MAGIC ##### 1. Ranking Functions
# MAGIC These functions evaluate positions within a partition based on the sorting sequence.
# MAGIC - row_number(): Assigns a sequential unique integer starting from 1.
# MAGIC - rank(): Assigns a rank, but leaves gaps in numbers if values tie.
# MAGIC - dense_rank(): Assigns a rank without skipping numbers on a tie.

# COMMAND ----------

"""
# Define window spec
ranking_window = Window.partitionBy("Department").orderBy(F.desc("Salary"))

# Apply ranking
df.withColumn("RowNumber", F.row_number().over(ranking_window)) \
  .withColumn("Rank", F.rank().over(ranking_window)) \
  .withColumn("DenseRank", F.dense_rank().over(ranking_window)) \
  .show()
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Analytical (Value) Functions
# MAGIC These functions fetch specific values relative to the position of your current row.
# MAGIC - lag(col, offset): Looks backward to pull a value from N rows prior.
# MAGIC - lead(col, offset): Looks forward to pull a value from N rows ahead

# COMMAND ----------

"""
# Define window spec ordered by Salary
analytic_window = Window.partitionBy("Department").orderBy("Salary")

# Fetch previous and next salaries
df.withColumn("PrevSalary", F.lag("Salary", 1).over(analytic_window)) \
  .withColumn("NextSalary", F.lead("Salary", 1).over(analytic_window)) \
  .show()

"""

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Aggregate Window Functions & Frames
# MAGIC You can apply standard aggregates (sum, avg, min, max) over a window. To build running totals or moving averages, you must explicitly bounded your frame.
# MAGIC - Window.unboundedPreceding: The first row of the partition.
# MAGIC - Window.currentRow: The row currently being processed.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Example 1

# COMMAND ----------

"""
# Cumulative Running Total Spec
running_total_window = Window.partitionBy("Department") \
                             .orderBy("Salary") \
                             .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df.withColumn("RunningTotal", F.sum("Salary").over(running_total_window)).show()

"""

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Example 2

# COMMAND ----------

windowSpec = Window.orderBy(col("quantity") * col("price")).rowsBetween(Window.unboundedPreceding, Window.currentRow)

cust_df = ( raw_cust_df.withColumns({ "REVENUE": col("quantity") * col("price"),
                                      "CUMU_REV" : sum(col("quantity") * col("price")).over(windowSpec)
                                    })
                       .select("ORDER_DATE","REVENUE", "CUMU_REV")
          ) 
cust_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Example 3

# COMMAND ----------

# Assuming raw_cust_df is your source DataFrame loaded from dev.spark_db.customers
# Step 1: Calculate revenue and aggregate
t1 = raw_cust_df.groupBy("QUANTITY", "PRICE") \
                .agg(expr("QUANTITY * PRICE AS REV")) \
                .groupBy("REV") \
                .agg(sum("REV").alias("REVENUE"))

# Step 2: Define a window for the cumulative sum
window_spec = Window.orderBy("REVENUE")

# Step 3: Apply the cumulative sum over the window
result = t1.withColumn("CUMU_REV", sum("REVENUE").over(window_spec)) \
           .select("REVENUE", "CUMU_REV")

result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### See folder CH08-Spark-Aggregates
# MAGIC
# MAGIC notebook: 04-Window Aggregate
# MAGIC
# MAGIC ###### Structure

# COMMAND ----------





from pyspark.sql.window    import Window        # Import window function
from pyspark.sql.functions import rank, col     # Import rank, and more functions


----------------------GENERAL STRUCTURE-----------------------
"""
  agg_function().OVER(Window.PARTITION_BY(column_list)
                            .ORDER_BY(column_list)
                            .ROWS_BETWEEN(window_start, window_end)
-----------------------------------------
"""

----------------------OPTION ONE: Use the window function directly in the transformation ----------------------


----------------------OPTION TWO: Save the window function to a variable, then use it in ----------------------
                                  the transformations in the result_df

# Create the window. save it to a variable, then use it in the result_df
"""
window_spec = (
    Window.partitionBy("booked_by")
        .orderBy(col("revenue").desc())
)

result_df = (
    booking_summary_df.withColumn("rank", rank().over(window_spec)) # Use the variable here
            .where("rank <= 3")
            .drop("rank")
)

result_df.display()
"""


# COMMAND ----------

# MAGIC %md
# MAGIC ##### 17.2 TBD

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------------------------
# MAGIC #### 18 
# MAGIC -----------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------------------------
# MAGIC #### 19 Temporary View
# MAGIC ##### Register a temporary View
# MAGIC -----------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Example:
# MAGIC ###### 1 Find duplicates
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ###### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT   customerid, count(*)
# MAGIC FROM     dev.spark_db.customers as C
# MAGIC GROUP BY customerid
# MAGIC HAVING   count(*) > 1

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark
# MAGIC
# MAGIC Method 1: Using DataFrame API (Recommended)
# MAGIC
# MAGIC Method 2: Using a TemporaryView and Spark SQL
# MAGIC
# MAGIC -------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 1: Using DataFrame API (Recommended)
# MAGIC This approach is the most common and "PySpark way" to achieve the result.

# COMMAND ----------

from pyspark.sql.functions import col, count, expr

grouped_df = ( raw_cust_df.groupBy("customerid")
                          .agg(expr("count(*) as customerids") )
                          .filter("customerids > 1") # This works as the 'having' filter for group by in SQL
             )

grouped_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Other ways

# COMMAND ----------

from pyspark.sql.functions import col, count


# 1. Group by 'customerid' and count the occurrences, giving the count column an alias
#    (e.g., 'customer_count')
grouped_df = (cust_df.groupBy("customerid") \
                     .agg(count("*").alias("customer_count")) #
             )
# 2. Filter the result to keep only rows where the count is greater than 1
result_df = grouped_df.filter(col("customer_count") > 1) #

# 3. Show the result
result_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 2: Using a TemporaryView and Spark SQL
# MAGIC
# MAGIC You can also use raw SQL directly within PySpark by registering your DataFrame as a temporary view.

# COMMAND ----------


# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame.
# Basically the "raw_cust_df" dataframe was built, using the FOOL format, from the customers.csv file 
raw_cust_df.createOrReplaceTempView("customers") # We give the view  a name 'customers'

# Run the SQL query using spark.sql()
sql_query = """
SELECT customerid, count(*) as customer_count
FROM customers
GROUP BY customerid
HAVING count(*) > 1
"""
result_df_sql = spark.sql(sql_query)

# Show the result
result_df_sql.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### remove dupllicates
# MAGIC
# MAGIC df.dropDuplicates(["id", "source", "destination"]).display()

# COMMAND ----------

raw_cust_df.dropDuplicates(["customerid", "departmentid"]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Calculate total revenue per product
# MAGIC
# MAGIC -------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SQL query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT product_name
# MAGIC       ,sum(quantity * price) as product_revenue 
# MAGIC       ,(sum(quantity * price) * 100.0 ) / (select sum(quantity * price) from dev.spark_db.customers) AS percent 
# MAGIC FROM dev.spark_db.customers as C
# MAGIC GROUP BY product_name
# MAGIC ;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Pyspark
# MAGIC
# MAGIC Method 1: Using DataFrame API (Recommended)
# MAGIC
# MAGIC Method 2: Using a TemporaryView and Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 1: Using DataFrame API (Recommended)
# MAGIC This approach is the most common  "PySpark way" to achieve the result.

# COMMAND ----------

from pyspark.sql.functions import expr, col, count, sum

# Get the toal revenue
#total_revenue = raw_cust_df.selectExpr('sum(quantity * price) as total_revenue').collect()[0]['total_revenue']
#total_revenue = raw_cust_df.agg(expr('sum(quantity * price) as total_revenue')).first()[0]
total_revenue = raw_cust_df.agg( sum(col('quantity') * col('price') )).first()[0]

result_df = ( raw_cust_df.groupBy("product_name")
                         .agg(  expr("sum(quantity * price) as product_revenue") 
                              ,(expr("sum(quantity * price) * 100.0") / total_revenue).alias("percent")
                             )
            )

result_df.display()   

# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 2: Using a TemporaryView and Spark SQL
# MAGIC
# MAGIC You can also use raw SQL directly within PySpark by registering your DataFrame as a temporary view.

# COMMAND ----------


# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame.
# Basically the "raw_cust_df" dataframe was built, using the FOOL format, from the customers.csv file 
raw_cust_df.createOrReplaceTempView('customers') # We give the view  a name 'customers'

# Run the SQL query using spark.sql()
sql_query = """
SELECT product_name
      ,sum(quantity * price) as product_revenue 
      ,(sum(quantity * price) * 100.0 ) / (select sum(quantity * price) from customers) AS percent 
FROM customers as C
GROUP BY product_name
"""
result_df = spark.sql(sql_query)
result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------------------------
# MAGIC #### 20 TBD
# MAGIC -----------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------------------------
# MAGIC #### 21 TBD
# MAGIC -----------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC -----------------------------------------------------------
# MAGIC #### 22 TBD
# MAGIC -----------------------------------------------------------

# COMMAND ----------

