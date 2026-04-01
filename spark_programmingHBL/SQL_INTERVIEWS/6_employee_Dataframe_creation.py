# Databricks notebook source
# MAGIC %md
# MAGIC ## Agenda
# MAGIC
# MAGIC The data resulting from each dataframe is then manually copied and pasted into a csv file since I cannot save to csv directly from here
# MAGIC
# MAGIC 1 Create spark session
# MAGIC
# MAGIC 2 create dataframe
# MAGIC   * employee
# MAGIC   * department
# MAGIC
# MAGIC 3 Save dataframe to csv file
# MAGIC   * employee
# MAGIC   * department
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

import pandas as pd
import duckdb
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import to_date, col

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2 Create dataframe: employee

# COMMAND ----------

# DBTITLE 1,Create employee DataFrame
data_schema = StructType([
              StructField("employee_id", IntegerType(), True),
              StructField("Name", StringType(), True),
              StructField("departmentid", IntegerType(), True),              
              StructField("salary", IntegerType(), True), 
              StructField("startdate", StringType(), True) ,
              StructField("enddate", StringType(), True) 
             ])

data_list  = [
              (1,'John Smith'     , 1,    60000, "2020-01-15", None),
              (2,'Sarah Johnson'  , 1,    65000, "2019-06-20", None),
              (3,'Michael Brown'  , 2,    75000, "2018-03-10", None),
              (4,'Emily White'    , 2,    70000, "2021-02-14", None),
              (5,'David Lee'      , 3,    80000, "2017-11-25", None),
              (6,'Jennifer Davis' , 3,    78000, "2019-09-01", "2023-03-30"),
              (7,'Robert Wilson'  , None, 55000, "2022-04-12", None),
              (8,'Lisa Anderson'  , 4,    72000, "2020-07-08", None ),
              (9,'James Taylor'   , 4,    71000, "2021-01-20", None),
              (10,'Mary Martinez' , None, 58000, "2022-05-15", None),
              (11,'Chris Garcia'  , 1,    60000, "2020-10-30", None),
              (12,'Paty Rodriz'   , 2,    76000, "2019-12-05", None),
              (13,'Daniel Thomn'  , None, 54000, "2023-02-01", None),
              (14,'Linda Harris'  , 3,    78000, "2018-08-19", None),
              (15,'Joseph Clark'  , 4,    73000, "2021-11-10", None),
              (16,'Barbara Lewis' , None, 56000, "2022-09-22", None),
              (17,'Thomas Walker' , 1,    63000, "2020-03-17", None ),
              (18,'Susan Hall'    , 2,    74000, "2019-07-11", None ),
              (19,'Charles Allen' , None, 59000, "2023-01-08", None),
              (20,'Nancy Young'   , 3,    81000, "2017-05-30", "2020-05-30"),
              (21,'Eva Young'     , 5,    80000, "2017-05-30", "2020-06-30"),
              (22,'Jeva Chandra'  , 5,    80000, "2017-08-20", None),
              (23,'Jim Gallion'   , 6,    61000, "2017-08-20", None),
              (24,'Juan Lopez'    , 7,    10000, "2018-03-22", None ),
              (25,'Mina Nakao'    , 8,    30000, "2018-03-20", None),
              (26,'Chad Roagen'   , 9,    30000, "2018-03-17", None),
              (27,'Lilly Raied'   , 10,   40000, "2017-08-20", None )   
             ]
df = spark.createDataFrame(data=data_list, schema=data_schema)
# Fix date format by first adding "" to convert date into string and then to date due to an isse of teh pspark version 4 not reading the date input and changing a;l dates to 1975
date_cols = {
             "startdate": to_date(col("startdate"), "yyyy-MM-dd"),
             "enddate":   to_date(col("enddate"), "yyyy-MM-dd")
            }

df_fixed =  df.withColumns(date_cols) 
df_fixed.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create dataframe: department

# COMMAND ----------

data_schema2 = StructType([
                           StructField("departmentname", StringType(), True),
                           StructField("departmentid", IntegerType(), True)
                          ])

data_list2  = [
              ('Sales'               ,1),
              ('Engineering'         ,2),
              ('HR'                  ,3),
              ('Marketing'           ,4),
              ('Finance'             ,5),
              ('Operations'          ,6),
              ('IT Support'          ,7),
              ('Legal'               ,8),
              ('Customer Service'    ,9),
              ('Product Management'  ,10)              
             ]
df2 = spark.createDataFrame(data=data_list2, schema=data_schema2)

df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3 Save dataframe to csv file
# MAGIC

# COMMAND ----------

