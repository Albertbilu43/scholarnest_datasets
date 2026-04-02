# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC <img src="https://learningjournal.github.io/pub-resources/logos/scholarnest_academy.jpg" alt="ScholarNest Academy" style="width: 1400px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. How to create Spark Session

# COMMAND ----------

# MAGIC %md
# MAGIC -- This option creates a session using the builder but the next option (below) uses a pre-cretaed version which needs less code
# MAGIC
# MAGIC from pyspark.sql import SparkSession
# MAGIC
# MAGIC spark_session = SparkSession.builder.getOrCreate()
# MAGIC
# MAGIC spark_session.version

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. What is pre-created Spark Session

# COMMAND ----------

#This below is a pre-created spark session. It's used when you want t avoid creating the spark session with teh builder. "spark" is teh precreated session
# We will use this to create a spark session from here onwards
spark.version

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. READ Employee csv
# MAGIC ##### FOOL(Format, Option, Option, Load)
# MAGIC spark.read.format().option().option().load()
# MAGIC
# MAGIC How to use SparkSession to read a csv file
# MAGIC
# MAGIC ##### At a first glance some data types would nneed to be fixed becasue of a bad schema inference

# COMMAND ----------

raw_emp_df= (
              spark.read.format('csv')
                   .option("header", "true")
                   .option("inferSchema", "true")
                   .load("/Volumes/dev/spark_db/datasets/spark_programming/data/employee.csv")
)

raw_emp_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Fix "departmentid" and "enddate" columns type to "Int" and "date" respectively.
# MAGIC After reading the data we noticed that "departmentid" and "enddate" are inferred incorrectly, both as "String" but it should be:
# MAGIC
# MAGIC departmentid ="Int"  and enddate = "date" so this needs to be fixed.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### From PySpark use either withColumn() or withColumns():
# MAGIC
# MAGIC 1 withColumn() to add a column or replacing the existing column that has the same name. 
# MAGIC
# MAGIC 2 withColumns() to add multiple columns or replacing the existing columns that have the same names.
# MAGIC
# MAGIC doc: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Fix departmentid and enddate types
from pyspark.sql.functions import expr, col
""" 
expr(str)          - Parses the expression string into the column that it represents
col(col)           - Returns a Column based on the given column name.
column(col)        - Returns a Column based on the given column name.
lit(col)           - Creates a Column of literal value.
try_cast(dataType) - A special version of cast that performs the same operation, but returns a NULL value instead of raising an error if 
                     the invoke method throws exception.
"""

emp_df = raw_emp_df.withColumns({
    'departmentid': expr("try_cast(departmentid as int)")
    #,'enddate': expr("try_to_date(enddate, 'M/d/yyyy')")
})

emp_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load data into the employee table

# COMMAND ----------

emp_df.write.mode("overwrite").saveAsTable("dev.spark_db.employee")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Query the table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) FROM dev.spark_db.employee  -- count all records
# MAGIC
# MAGIC --df = spark.table("dev.spark_db.employee")
# MAGIC --df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Department table 
# MAGIC How to use SparkSession to read a csv file
# MAGIC
# MAGIC ##### At a first glance data looks fine

# COMMAND ----------

raw_dep_df= (
              spark.read.format('csv')
                   .option("header", "true")
                   .option("inferSchema", "true")
                   .load("/Volumes/dev/spark_db/datasets/spark_programming/data/department.csv")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Data is fine so no fixes needed.
# MAGIC

# COMMAND ----------

dep_df= raw_dep_df
dep_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Load data into department table

# COMMAND ----------

# DBTITLE 1,Untitled
dep_df.write.mode("overwrite").saveAsTable("dev.spark_db.department")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query the table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) FROM dev.spark_db.department  -- count all records
# MAGIC
# MAGIC --df = spark.table("dev.spark_db.employee")
# MAGIC --df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2021-2026 <a href="https://www.scholarnest.com/">ScholarNest</a>. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation.</a><br/>
# MAGIC Databricks, Databricks Cloud and the Databricks logo are trademarks of the <a href="https://www.databricks.com/">Databricks Inc.</a><br/>
# MAGIC <a href="https://www.scholarnest.com/pages/privacy">Privacy Policy</a> | <a href="https://www.scholarnest.com/pages/terms">Terms of Use</a> | <a href="https://www.scholarnest.com/pages/contact">Contact Us</a>
# MAGIC