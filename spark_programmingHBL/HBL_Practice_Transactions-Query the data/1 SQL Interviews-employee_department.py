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
# MAGIC     Fix data types errors with:
# MAGIC
# MAGIC     Single column    --> .withColumn()     col().cast() or use this expr('cast()')
# MAGIC
# MAGIC     Multiple columns --> .withColumns({ }) col().cast() or use this expr('cast()')
# MAGIC
# MAGIC ------------------------------------------------------------------------------
# MAGIC
# MAGIC     example: SINGLE COLUMN
# MAGIC        # Fix the order_date as it was incorrectly inferred
# MAGIC        df_fixed2 =  df2_raw.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")  ) 
# MAGIC ------------------------------------------------------------------------------
# MAGIC     example: MULTIPLE COLUMNS
# MAGIC        # FIX MORE THAN ONE COLUMN at the same time
# MAGIC        .
# MAGIC        #-------------------OPTION ONE --Using col()----------------
# MAGIC        #df_fixed2 =  df2_raw.withColumns({"order_date": to_date(col("order_date"), "yyyy-MM-dd"),
# MAGIC        #                                  "order_id": col("order_id").cast("string")
# MAGIC                                          #"order_id": col("order_id").try_cast(IntegerType())  try_cast returns null if an error hapens  
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
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
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

# COMMAND ----------

# MAGIC %md
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
from pyspark.sql.functions import to_date, col, expr, try_to_date, regexp_replace

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
# MAGIC ##### <> using spark.read.format (FOOL)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC  <> create a Dataframe by reading a file (in the Volume a csv, json, etc.) 
# MAGIC -     using spark.read.format
# MAGIC                       .option
# MAGIC                       .option
# MAGIC                       .load         

# COMMAND ----------

# MAGIC %md
# MAGIC ### EMPLOYEE

# COMMAND ----------


raw_emp_df = ( spark.read.format('csv')
                          .option("header", True)
                          .option("inferSchema", True)
                          .load("/Volumes/dev/spark_db/datasets/spark_programming/data/employee.csv")
              )

raw_emp_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### 2.1 Eyeball the data to spot possible issues

# COMMAND ----------

# MAGIC %md
# MAGIC ------------------------------------------------------------------
# MAGIC #####2.2 Fix any data issues discovered.
# MAGIC After reading the data we noticed that one issue needs to be solved.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC  In case you need to make fixes you can make use of these : 
# MAGIC
# MAGIC  From PySpark use either withColumn() or withColumns()
# MAGIC
# MAGIC ##### 1 withColumn() to add a column or replacing the existing column that has the same name. 
# MAGIC
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withColumn.html
# MAGIC
# MAGIC ##### 2 withColumns() to add multiple columns or replacing the existing columns that have the same names.
# MAGIC
# MAGIC doc: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html
# MAGIC
# MAGIC

# COMMAND ----------

fix_emp_df= raw_emp_df.withColumns({ "departmentid": col("departmentid").try_cast(IntegerType())                                       
                                     #,"enddate": to_date(col("enddate"), "yyyy-MM-dd")
                                   }
                                  )
fix_emp_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DEPARTMENT 

# COMMAND ----------

raw_dep_df = ( spark.read.format('csv')
                         .option("header", True)
                         .option("inferSchema", True)
                         .load("/Volumes/dev/spark_db/datasets/spark_programming/data/department.csv")
              )

raw_dep_df.display()

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
# MAGIC ![image_1774032783996.png](./image_1774032783996.png "image_1774032783996.png")
# MAGIC
# MAGIC #### 10-50 SQL Interview Questions 
# MAGIC
# MAGIC https://www.linkedin.com/feed/update/urn:li:activity:7402591561454383104/?updateEntityUrn=urn%3Ali%3Afs_updateV2%3A%28urn%3Ali%3Aactivity%3A7402591561454383104%2CFEED_DETAIL%2CEMPTY%2CDEFAULT%2Cfalse%29
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC
# MAGIC #### Second highest salary by employee
# MAGIC ------------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MAX(SALARY) as SECOND_TOP_SALARY
# MAGIC FROM   dev.spark_db.employee 
# MAGIC WHERE SALARY < (SELECT MAX(SALARY) FROM dev.spark_db.employee)

# COMMAND ----------

# MAGIC %md
# MAGIC ------------------------------------------------------------------
# MAGIC ##### PySpark
# MAGIC
# MAGIC ![image_1774556836253.png](./image_1774556836253.png "image_1774556836253.png")
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 1 Read teh data.

# COMMAND ----------

# 1 This steps is done when we created the dataframe "raw_emp_df" by reading a file from Volume

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 2 Apply transformations(Composable query: All steps in encapsualted) 
# MAGIC
# MAGIC ##### Now you have to use the newly created dataframe "raw_emp_df"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #####Remember: If using select() and selectExpr() 
# MAGIC   - select() uses the DataFrame API's column objects and functions
# MAGIC   - selectExpr() accepts SQL-style expressions as strings.
# MAGIC     -     But be careful when using groupBy() and agg(). See the notebook "Instructions..."
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###STANDARD WAY
# MAGIC ##### The piece of code below is the STANDARD WAY of performing groupBy and aggregations 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Option One

# COMMAND ----------


from pyspark.sql.functions import to_date, col, expr, sum, count, max 

# In PySpark, the sequence agg(...).first()[0] is a common pattern used to extract a single scalar value from an aggregated DataFrame.
max_salary = raw_emp_df.agg(max("salary")).first()[0] # max_salary

# Then filter for salaries less than max and get the second highest
result_df = (raw_emp_df.filter(col("salary") < max_salary)
                       .agg(max("salary").alias("SECOND_TOP_SALARY"))
            )

result_df.display()

# First, get the maximum salary
"""
Desglose del código:

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
# MAGIC ##error:
# MAGIC ##### This piece of code below will produce an error cause 
# MAGIC
# MAGIC you cannot replicate the query directly using a selectExper() as it requires the registration of the dataframe as a temporary view first
# MAGIC
# MAGIC
# MAGIC Use the standard way instead

# COMMAND ----------

"""
from pyspark.sql.functions import to_date, col, expr, sum, count

result_df =(  raw_cust_df.selectExpr("productid" ,"sum(quantity * price) as Revenue")
                         .orderBy("Revenue", ascending=False)
                         .limit(3)
           )
            
result_df.display()
"""       

# COMMAND ----------

# MAGIC %md
# MAGIC #### Option Two
# MAGIC
# MAGIC Creating a temporary view

# COMMAND ----------

from pyspark.sql.functions import expr, col, count

# Assuming 'raw_emp_df.createOrReplaceTempView("employee")' is your PySpark DataFrame
raw_emp_df.createOrReplaceTempView("employee")

# Run the SQL query using spark.sql()
sql_query = """
SELECT MAX(SALARY) as SECOND_TOP_SALARY
FROM employee
WHERE SALARY < (SELECT MAX(SALARY) FROM employee)
"""
result_df_sql = spark.sql(sql_query)

# Show the result
result_df_sql.show()

# COMMAND ----------

"""from pyspark.sql.functions import to_date, col, expr, sum, count


result_df =(  raw_cust_df.groupBy("productid")
                         .agg(expr("sum(quantity * price)  as Revenue"))
                         .orderBy("Revenue", ascending=False)
                         .limit(3)
           )
            
result_df.display()
"""


# COMMAND ----------

"""
from pyspark.sql.functions import expr

result_df = ( cust_df.select("customerid", "customer_name", "productid", "quantity", "price")
                     .where("productid IS NOT NULL")
                     .groupBy("productid").agg(expr("sum(quantity * price)").alias("sales"))
                     .orderBy("sales", ascending=False)
                     .limit(3)
            )

result_df.display()            
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Find employees without department
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT EMPLOYEE_ID
# MAGIC FROM dev.spark_db.employee as e
# MAGIC WHERE DEPARTMENTID IS NULL

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
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------


from pyspark.sql.functions import col, expr, regexp_replace

result_emp_df =(  raw_emp_df.where(col("departmentid").isNull())
                            .select(col("employee_id"), col("name"))
               ) 
result_emp_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------

from pyspark.sql.functions import col, expr

#Create a temporaryView
raw_emp_df.createOrReplaceTempView("employee")

sql_query= """
          SELECT EMPLOYEE_ID
          FROM employee
          WHERE DEPARTMENTID IS NULL
           """

result_emp_df= spark.sql(sql_query)
result_emp_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Find top-3 Higest-paid employees
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT Name, salary 
# MAGIC FROM dev.spark_db.employee
# MAGIC ORDER BY salary DESC
# MAGIC limit 3;

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
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------

from pyspark.sql.functions import expr, col, count

grouped_df= (raw_emp_df.select(col("Name"), col("salary") )
                       .orderBy('salary', ascending=False)
                       .limit(3)
            )

grouped_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------


# Assuming 'raw_emp_df.createOrReplaceTempView("employee")' is your PySpark DataFrame.
# Basically the "raw_emp_df" dataframe was built, using the FOOL format, from the employee.csv file 
raw_emp_df.createOrReplaceTempView("employee")

# Run the SQL query using spark.sql()
sql_query ="""
SELECT Name, salary 
FROM employee
ORDER BY salary DESC
limit 3;
"""

result_df = spark.sql(sql_query)
result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Retrieve all employees who joined in 2020
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT E.name
# MAGIC FROM      dev.spark_db.employee  as E
# MAGIC WHERE extract(year from E.startdate) = 2020

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
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------


from pyspark.sql.functions import expr, col, count, year

new_df= (raw_emp_df.filter(year(col("startdate")) == 2020) # Using the function year()
                   .select(col("Name"))
                   .distinct()
            )

new_df.display()


#new_df= (raw_emp_df.where(expr("year(startdate) = 2020")) # Using the function expr() and year()
#                   .select(col("name"))
#                   .distinct()
#            )

#new_df.display()



# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------


raw_emp_df.createOrReplaceTempView("employee")

sql_query ="""
SELECT DISTINCT E.name
FROM     employee  as E
WHERE extract(year from E.startdate) = 2020
"""
result_df =  spark.sql(sql_query)
result_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Find All employees with salary between 65000 and 80000
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC                SELECT  name, salary
# MAGIC                FROM   dev.spark_db.employee    AS E
# MAGIC                WHERE salary BETWEEN 75000 AND 80000
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
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------

from pyspark.sql.functions import expr, col, count, max, sum

result_df =  ( raw_emp_df.where( (col("salary") >= 75000) & (col("salary") <= 80000) )
                         .select("name", "salary")
             )

result_df.display()

#result_df =  ( raw_emp_df.select( col("name"), col("salary") )
#                         .where( col("salary").between(65000, 80000) )
#             )

#result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------

raw_emp_df.createOrReplaceTempView("employee")

sql_query ="""
               SELECT  name, salary
               FROM   employee    AS E
               WHERE salary BETWEEN 75000 AND 80000
;
"""
result_df =  spark.sql(sql_query)
result_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC #### Rank employees by salary within each department
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC                SELECT  departmentid, name, salary
# MAGIC                       ,DENSE_RANK() OVER(PARTITION BY departmentid ORDER BY salary DESC) AS salary_rank
# MAGIC                FROM   dev.spark_db.employee    AS E
# MAGIC                ORDER BY departmentid, salary_rank ASC
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
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------

from pyspark.sql.functions import expr, col, count, max, sum

result_df =  ( raw_emp_df.withColumn("salary_rank", expr("DENSE_RANK() OVER(PARTITION BY departmentid ORDER BY salary DESC)"))
                         .select(col("departmentid"), col("name"), col("salary"), col("salary_rank"))
                         .orderBy(col("departmentid"), col("salary_rank").asc())
             )

result_df.display()


#result_df =  ( raw_emp_df.select( col("departmentid"), col("name"), col("salary")
#                            ,expr("DENSE_RANK() OVER(PARTITION BY departmentid ORDER BY salary DESC) AS salary_rank")
#                            )
#                .orderBy(col("departmentid"), col("salary_rank").asc())
#             )
#result_df.display()



# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------

# Assuming 'raw_emp_df.createOrReplaceTempView("employee")' is your PySpark DataFrame.
# Basically the "raw_emp_df" dataframe was built, using the FOOL format, from the employee.csv file 
raw_emp_df.createOrReplaceTempView("employee")

# Run the SQL query using spark.sql()
sql_query ="""
               SELECT  departmentid, name, salary
                      ,DENSE_RANK() OVER(PARTITION BY departmentid ORDER BY salary DESC) AS salary_rank
               FROM   employee    AS E
               ORDER BY departmentid, salary_rank ASC
;
"""

result_df = spark.sql(sql_query)
result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Find percentage of employees in each department
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC Select departmentid, count(employee_id) as emp_cnt
# MAGIC       ,(SELECT COUNT(*) FROM dev.spark_db.employee ) AS TOTAL_EMP_CNT
# MAGIC       ,(count(employee_id) * 100.0 / (SELECT COUNT(*) FROM dev.spark_db.employee) ) AS PERCENTAGE
# MAGIC FROM dev.spark_db.employee as E
# MAGIC GROUP BY departmentid

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
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------



from pyspark.sql.functions import expr, col, count, min, max, sum, avg
from pyspark.sql.window import Window


result_df =  ( raw_emp_df.groupBy("departmentid")
                         .agg(count("employee_id").alias("emp_cnt") )
                         .withColumn("TOTAL_EMP_CNT", sum("emp_cnt").over(Window.partitionBy()) ) # Window.partitionBy(): Definir una ventana vacía es la forma más limpia en                                                                         en PySpark de decirle al motor: "calcula esto sobre todo el dataset".
                         .withColumn("PERCENTAGE", col("emp_cnt") * 100.0 / col("TOTAL_EMP_CNT") )
             )


result_df.display()

"""



-- FIXED CODE !!!!
Fix explanation:

¿Qué cambió?F.sum("emp_cnt"): En lugar de contar filas, sumamos los conteos previos para tener el universo total de empleados.Window.partitionBy(): Definir una ventana vacía es la forma más limpia en PySpark de decirle al motor: "calcula esto sobre todo el dataset".Alias F: Es una buena práctica importar pyspark.sql.functions como F para evitar conflictos con funciones nativas de Python.¿Te gustaría que ajustemos el código para redondear el porcentaje a dos decimales o prefieres mantener la precisión completa?



from pyspark.sql import functions as F
from pyspark.sql.window import Window

result_df = (
    raw_emp_df.groupBy("departmentid")
    .agg(F.count("employee_id").alias("emp_cnt"))
    # Usamos F.sum() sobre una ventana vacía para obtener el total global de empleados
    .withColumn("TOTAL_EMP_CNT", F.sum("emp_cnt").over(Window.partitionBy()))
    .withColumn("PERCENTAGE", F.col("emp_cnt") * 100.0 / F.col("TOTAL_EMP_CNT"))
)




-- ORIGINAL CODE WITH ERROR!!!!
Error explanation:

Tu código tiene un pequeño error lógico en la ventana (window function). Al usar expr("count(*) over()"), PySpark contará el número de filas (es decir, el número de departamentos) en lugar de la suma total de empleados.Para obtener el porcentaje real de empleados por departamento, debes sumar la columna emp_cnt. Aquí tienes la corrección:

result_df =  ( raw_emp_df.groupBy("departmentid")
                         .agg(count("employee_id").alias("emp_cnt"))
                         .withColumn("TOTAL_EMP_CNT", expr("count(*) over() ") )
                         .withColumn("PERCENTAGE", col("emp_cnt") * 100.0 / col("TOTAL_EMP_CNT") )
             )
 in pyspark




"""

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------

# Assuming 'raw_emp_df.createOrReplaceTempView("employee")' is your PySpark DataFrame.
# Basically the "raw_emp_df" dataframe was built, using the FOOL format, from the employee.csv file 
raw_emp_df.createOrReplaceTempView("employee")

# Run the SQL query using spark.sql()
sql_query ="""
Select departmentid, count(employee_id) as emp_cnt
      ,(SELECT COUNT(*) FROM dev.spark_db.employee ) AS TOTAL_EMP_CNT
      ,(count(employee_id) * 100.0 / (SELECT COUNT(*) FROM dev.spark_db.employee) ) AS PERCENTAGE
FROM dev.spark_db.employee as E
GROUP BY departmentid
;
"""

result_df = spark.sql(sql_query)
result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### Retrieve the maximun salary difference within each department
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

# COMMAND ----------

# MAGIC %sql
# MAGIC Select departmentid
# MAGIC       ,MAX(salary) - MIN(salary) AS SALARY_DIFF
# MAGIC FROM dev.spark_db.employee as E
# MAGIC GROUP BY departmentid
# MAGIC ;

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
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------

from pyspark.sql.functions import expr, col, count, min, max, sum, avg, desc, asc
#from pyspark.sql.window import Window


result_df =  ( raw_emp_df.groupBy("departmentid")
                         .agg( (max("salary") - min("salary")).alias("SALARY_DIFF") )
                        .orderBy( asc( col("departmentid") ))
             )


result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------

# Assuming 'raw_emp_df.createOrReplaceTempView("employee")' is your PySpark DataFrame.
# Basically the "raw_emp_df" dataframe was built, using the FOOL format, from the employee.csv file 
raw_emp_df.createOrReplaceTempView("employee")

# Run the SQL query using spark.sql()
sql_query ="""
Select departmentid
      ,MAX(salary) - MIN(salary) AS SALARY_DIFF
FROM dev.spark_db.employee as E
GROUP BY departmentid
;
"""

result_df = spark.sql(sql_query)
result_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### TBD
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

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
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774032826788.png](./image_1774032826788.png "image_1774032826788.png")
# MAGIC #### TBD
# MAGIC
# MAGIC -------------------------------------------------------------
# MAGIC ##### SQL Query

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
# MAGIC ##### Option One
# MAGIC ###### Using DataFrame API (Recommended)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Option Two
# MAGIC ###### Using a TemporaryView and Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC --------------------------------------------------------
# MAGIC ##### Others

# COMMAND ----------

from pyspark.sql.functions import col, count


# 1. Group by 'customerid' and count the occurrences, giving the count column an alias
#    (e.g., 'customer_count')
grouped_df = (raw_cust_df.groupBy("customerid") \
                        .agg(count("*").alias("customer_count")) #
             )
# 2. Filter the result to keep only rows where the count is greater than 1
result_df = grouped_df.filter(col("customer_count") > 1) #

# 3. Show the result
result_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ######Method 2: Using Spark SQL
# MAGIC
# MAGIC You can also use raw SQL directly within PySpark by registering your DataFrame as a temporary view.

# COMMAND ----------

from pyspark.sql.functions import expr, col, count

# Assuming 'raw_cust_df.createOrReplaceTempView("customers")' is your PySpark DataFrame
raw_cust_df.createOrReplaceTempView("customers")

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
# MAGIC ### Practica #1: Number of distint products types

# COMMAND ----------

# MAGIC %md
# MAGIC #### With SQL query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT PRODUCTID) AS DISTINCT_PRODUCT_COUNT 
# MAGIC FROM dev.spark_db.customers
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### With Pyspark transformation
# MAGIC
# MAGIC ##### Take the same piece of the SQL query above : 
# MAGIC
# MAGIC COUNT(DISTINCT PRODUCTID) AS DISTINCT_PRODUCT_COUNT 
# MAGIC
# MAGIC ##### and, in pyspark, put into the:
# MAGIC
# MAGIC  selectExpr('COUNT(DISTINCT PRODUCTID) AS DISTINCT_PRODUCT_COUNT ')

# COMMAND ----------

res_cust_df = (cust_df.selectExpr('count(distinct productid) as distinct_product_count') 
              )
res_cust_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Practice#2: Customers with consecutive purchases (2 days)

# COMMAND ----------

# MAGIC %md
# MAGIC #### SQL query

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH Prev_pur as ( SELECT CUSTOMERID, ORDER_ID, ORDER_DATE, LAG(ORDER_DATE) OVER( ORDER BY ORDER_DATE ) AS Prev_purchase 
# MAGIC                    FROM dev.spark_db.customers
# MAGIC                    ORDER BY CUSTOMERID
# MAGIC                  )
# MAGIC              SELECT *
# MAGIC              FROM Prev_pur
# MAGIC              WHERE (PREV_PURCHASE + INTERVAL 1 DAY) = ORDER_DATE
# MAGIC              --DATEDIFF(ORDER_DATE , Prev_purchase ) =1 
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark

# COMMAND ----------

query = """
        WITH Prev_pur as ( SELECT CUSTOMERID, ORDER_ID, ORDER_DATE, LAG(ORDER_DATE) OVER( ORDER BY ORDER_DATE ) AS Prev_purchase 
                   FROM dev.spark_db.customers
                   ORDER BY CUSTOMERID
                 )
             SELECT *
             FROM Prev_pur
             WHERE (PREV_PURCHASE + INTERVAL 1 DAY) = ORDER_DATE
             --DATEDIFF(ORDER_DATE , Prev_purchase ) =1 
        """
result= spark.sql(query)
display(result)

# COMMAND ----------


data_schema = "id int, source string , destination string, distance int"

data_list= [(101, "Mumbai", "Goa", 587),
            (102, "Mumbai", "Bangalore", 985),
            (102, "Mumbai", "Bangalore", 985),
            (103, "Dheli", "Chennai", 2208),
            (104, "Dheli", "Chennai", 2208),     
            (105, "Bangalore", "Kolkata", 1868),             
            (105, "Bangalore", "Kolkata", 1865)                           
            ]
#df = spark.createDataFrame(data=data_list, schema=data_schema)
df=pd.DataFrame(data_list)

df.display()

# COMMAND ----------

query = """
SELECT id, source
from df 
"""

result = duckdb.query(query).df()
print(result)

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

