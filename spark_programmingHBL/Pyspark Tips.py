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
# MAGIC ![image_1774986271380.png](./image_1774986271380.png "image_1774986271380.png")
# MAGIC
# MAGIC ### 3 Pyspark Tips
# MAGIC
# MAGIC '
# MAGIC
# MAGIC ![image_1774641851028.png](./image_1774641851028.png "image_1774641851028.png")
# MAGIC #### 1 Methods for dataframes(read, format, etc)
# MAGIC       spark.read.  Returns a DataFrameReader that can be used to read data in a DataFrame. 
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
# MAGIC #### 2 select() vs selectExtr()
# MAGIC   -     Both methods return a new DataFrame and are used for selecting and transforming columns
# MAGIC
# MAGIC ##### The core difference is that
# MAGIC        * select() uses PySpark's Column API with expressions
# MAGIC        * selectExpr() accepts standard SQL expressions as strings.
# MAGIC
# MAGIC """
# MAGIC     <<<<<<<<<<<<<<<<<<<<<<<expr() vs. selectExpr()>>>>>>>>>>>>>>>>>>>>
# MAGIC
# MAGIC
# MAGIC * expr()              operates on a single SQL expression and returns a Column object for use within other DataFrame methods, 
# MAGIC * selectExpr()        Accepts one or more SQL expressions as strings and applies them across the entire DataFrame in a single call,
# MAGIC                       returning a new DataFrame. 
# MAGIC                       <<python>>
# MAGIC                       # Using selectExpr to perform multiple operations at once
# MAGIC                       df_transformed = df.selectExpr( "name",
# MAGIC                                                      "Balance * 1.02 AS Adjusted_Balance",
# MAGIC                                                       "CASE WHEN Balance > 100000 THEN 'High' ELSE 'Low' END AS Balance_Level"
# MAGIC                                                     )
# MAGIC
# MAGIC * col(col)           - Se utiliza para hacer referencia a una columna de un DataFrame por su nombre, devolviendo un objeto de 
# MAGIC                        tipo Column. 
# MAGIC                        Es fundamental para transformaciones, filtros y ordenaciones al convertir cadenas de texto en expresiones de columna. 
# MAGIC                        col("nombre_columna").
# MAGIC * column(col)        - Returns a Column based on the given column name.
# MAGIC * lit(col)           - Creates a Column of literal value.
# MAGIC * try_cast(dataType) - A special version of cast that performs the same operation, but returns a NULL value instead of raising 
# MAGIC                        an error if the invoke method throws exception.
# MAGIC """
# MAGIC
# MAGIC -----------------------------------------------------------------------------------------
# MAGIC
# MAGIC ![image_1774458253140.png](./image_1774458253140.png "image_1774458253140.png")
# MAGIC ![image_1774457128402.png](./image_1774457128402.png "image_1774457128402.png")
# MAGIC
# MAGIC '
# MAGIC
# MAGIC ---------------------------------------------------------------------
# MAGIC ####1.1 select()
# MAGIC Key Point: select() is useful when you need to perform column selection or basic transformations.
# MAGIC ![image_1774457763357.png](./image_1774457763357.png "image_1774457763357.png")
# MAGIC
# MAGIC ###### Examples
# MAGIC
# MAGIC ![image_1774458064009.png](./image_1774458064009.png "image_1774458064009.png")
# MAGIC
# MAGIC '
# MAGIC '
# MAGIC
# MAGIC ---------------------------------------------------------------------
# MAGIC
# MAGIC ####1.2 selectExpr()
# MAGIC Key Point: selectExpr() is powerful for complex transformations, calculations, or applying SQL functions inline, allowing more flexibility in how data is selected and manipulated.
# MAGIC
# MAGIC ![image_1774457783339.png](./image_1774457783339.png "image_1774457783339.png")
# MAGIC
# MAGIC ###### Examples
# MAGIC
# MAGIC ![image_1774458087222.png](./image_1774458087222.png "image_1774458087222.png")
# MAGIC
# MAGIC .
# MAGIC
# MAGIC ####'where' SQL keyword 
# MAGIC ##### CANNOT BE used directly inside selectExpr() in PySpark to filter rows. 
# MAGIC  
# MAGIC     The selectExpr() method is designed for selecting columns and applying SQL expressions to those 
# MAGIC     columns(e.g., transformations, aggregations, or conditional logic with CASE WHEN), not to filter the dataset  
# MAGIC
# MAGIC
# MAGIC ![image_1774727977650.png](./image_1774727977650.png "image_1774727977650.png")
# MAGIC ![image_1774728759270.png](./image_1774728759270.png "image_1774728759270.png")
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### GROUP BY operation directly in selectExpr(). First need to register your DataFrame as a temporary SQL view 
# MAGIC
# MAGIC   -     Using selectExper() To calculate the revenue for each productid in PySpark, you first need to register your DataFrame as a temporary SQL view 
# MAGIC         and then execute a SQL query. 
# MAGIC
# MAGIC         PySpark's selectExpr() is designed to run SQL expressions on an existing DataFrame, not to perform a 
# MAGIC         GROUP BY operation directly.
# MAGIC
# MAGIC .
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ![image_1774641851028.png](./image_1774641851028.png "image_1774641851028.png")
# MAGIC
# MAGIC #### 3 groupBy() and agg()
# MAGIC
# MAGIC .
# MAGIC
# MAGIC ### selectExpr() 
# MAGIC ##### In PySpark  You can use selectExpr() to perform aggregations in two primary ways: 
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
# MAGIC ##### This is teh standard way in pyspark to groupe aggregations. THIS PIECE CONTAINS A field and aggregations but the field is grouped first
# MAGIC
# MAGIC ![image_1774468900290.png](./image_1774468900290.png "image_1774468900290.png")
# MAGIC
# MAGIC ![image_1774547384147.png](./image_1774547384147.png "image_1774547384147.png")
# MAGIC
# MAGIC
# MAGIC
# MAGIC ##### using selectExpr() after groupBy()
# MAGIC If you prefer to use selectExpr() after the groupBy(), you would typically register the DataFrame as a temporary view first, and then run a SQL query:

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
# MAGIC ###### This piece of teh above code DOES NOT CONTAINM A FIELD and aggregations, ONLY AGGREGATIONS
# MAGIC
# MAGIC ![image_1774468850948.png](./image_1774468850948.png "image_1774468850948.png")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Pay attention to the output as the last piece DOES NOT CONTAIN fields, just the entire aggregation
# MAGIC ![image_1774468340248.png](./image_1774468340248.png "image_1774468340248.png")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ![image_1774641851028.png](./image_1774641851028.png "image_1774641851028.png")
# MAGIC #### 4  where/filter/having

# COMMAND ----------

# MAGIC %md
# MAGIC ##### In PySpark
# MAGIC ###### where() and filter() 
# MAGIC Both are used to filter rows based on a given condition, similar to the SQL WHERE clause. They are interchangeable and can accept boolean expressions or SQL-style string conditions
# MAGIC
# MAGIC ###### having() 
# MAGIC In PySpark, 'having' IS NOT USED. You have two options instead:
# MAGIC   -     Applying a filter() or where() condition after the groupBy() and aggregation steps have been performed
# MAGIC   -     Using SQL queries directly OR
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SQL Query
# MAGIC
# MAGIC ![image_1774650780213.png](./image_1774650780213.png "image_1774650780213.png")
# MAGIC
# MAGIC .
# MAGIC
# MAGIC #####Pyspark
# MAGIC ![image_1774650889201.png](./image_1774650889201.png "image_1774650889201.png")
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ![image_1774641851028.png](./image_1774641851028.png "image_1774641851028.png")
# MAGIC #### 5 regexp_replace()  

# COMMAND ----------

# MAGIC %md
# MAGIC ##### To replace strings in PySpark, 
# MAGIC the most efficient methods use built-in functions like
# MAGIC regexp_replace for pattern matching or the DataFrame.replace method for direct value substitution.
# MAGIC
# MAGIC ![image_1774889319107.png](./image_1774889319107.png "image_1774889319107.png")
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774889389313.png](./image_1774889389313.png "image_1774889389313.png")

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774889417651.png](./image_1774889417651.png "image_1774889417651.png")
# MAGIC
# MAGIC ![image_1774889455830.png](./image_1774889455830.png "image_1774889455830.png")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ![image_1774641851028.png](./image_1774641851028.png "image_1774641851028.png")
# MAGIC #### 6  cast(), try_cast()
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ![image_1774641851028.png](./image_1774641851028.png "image_1774641851028.png")
# MAGIC ####     7  casting to date fails
# MAGIC
# MAGIC A string field containing date values often cannot be cast directly in PySpark using a simple.cast("date") because PySpark expects a default, ISO-compliant date format of yyyy-MM-dd. 
# MAGIC
# MAGIC If your input string is in any other format (e.g., MM/dd/yyyy, dd-MM-yyyy, or yyyyMMdd), the direct cast will likely result in null values or a SparkDateTimeException error.

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

