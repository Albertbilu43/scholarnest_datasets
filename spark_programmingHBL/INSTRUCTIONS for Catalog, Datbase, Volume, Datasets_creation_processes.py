# Databricks notebook source
# MAGIC %md
# MAGIC ##Agenda
# MAGIC
# MAGIC ### 1 Setup
# MAGIC      1- Create a Catalog, Database(Schema), Volume 
# MAGIC      2- Import Data Ingestion
# MAGIC ### 2 Creation of Tables/Dataframes
# MAGIC      1- Tables
# MAGIC      2- Dtaframes
# MAGIC      3- Upload data to tables(previously created)
# MAGIC
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
# MAGIC ### 1 Setup
# MAGIC
# MAGIC ##### 1- Create Catalog, DataBase(Schema), Volume
# MAGIC    - See Folder  CH02 Setup. 
# MAGIC    
# MAGIC          Notebook "01-setup-environment"
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ##### 2- Import Data Ingestion
# MAGIC ------------------------------------------------
# MAGIC ##### Remember. Data  can also be uploaded to the Volume by other methods( e.i. uploading a file from the computer to the volume)
# MAGIC ------------------------------------------------
# MAGIC    - See Folder  CH02 Setup. 
# MAGIC    
# MAGIC We use the "utils" folder, with a data-ingestion notebook inside.
# MAGIC This notebook contains contains 
# MAGIC -     a class DataLoader() and 
# MAGIC -     a method clean_ingest_data("spark_programming/data", "spark_programming/data") 
# MAGIC that would pull the datsets from a GitHub account and will import it to the volume we created.
# MAGIC
# MAGIC
# MAGIC    - See Folder  utils 
# MAGIC    
# MAGIC          Notebook "data-ingestion"
# MAGIC
# MAGIC For this you wil need to pip install and import requests and json
# MAGIC   -   Installings
# MAGIC
# MAGIC                pip3 install requests
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ![image_1774986271380.png](./image_1774986271380.png "image_1774986271380.png")
# MAGIC ### 2 Creation of tables/Dataframes
# MAGIC
# MAGIC Thes tables are the ones to be filled with either the data pulled from the import data ingestion or by the files we upload from our computer

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 1 Create empty tables for employees and departments
# MAGIC
# MAGIC     Notebook: employee_department_Create_tables_creation
# MAGIC
# MAGIC 2 Create empty tables for customers, orders, products, returns
# MAGIC
# MAGIC     Notebook: customers_orders_products_returns_tables_Creation
# MAGIC
# MAGIC 3 Create dataframes for employees and departments
# MAGIC
# MAGIC     NOtebook: employee_department_dataframe_creation. 
# MAGIC               You can directly WORK with these dataframes or 
# MAGIC               Run teh dataframe, copy the data and paste it in a csv file. Then upload the csv to the VOLUME and use it from there
# MAGIC
# MAGIC               UPLOAD -->
# MAGIC               Click on  to Catalog (dev) , spark_dv, Volumes, Datasets, spark_programming, select 'data' folder.
# MAGIC               If the file exists then delete it firt: Mark the chekbox next to the desired cvs file and click delete
# MAGIC               If teh file does not exist then upload it: Click on the top-right "Upload to Volume' bottom and upload files              
# MAGIC
# MAGIC 4 Create dataframes for customers, orders, products, returns 
# MAGIC
# MAGIC     Notebook: customer_products_orders_returns_dataframe_Creation
# MAGIC               You can directly WORK with these dataframes or 
# MAGIC               Run teh dataframe, copy the data and paste it in a csv file. Then upload the csv to the VOLUME and use it from there
# MAGIC               
# MAGIC               UPLOAD -->
# MAGIC               Click on  to Catalog (dev) , spark_dv, Volumes, Datasets, spark_programming, select 'data' folder.
# MAGIC               If the file exists then delete it firt: Mark the chekbox next to the desired cvs file and click delete
# MAGIC               If teh file does not exist then upload it: Click on the top-right "Upload to Volume' bottom and upload files
# MAGIC
# MAGIC 5 Upload the data from those csv files to each of teh tables previously created employee/department
# MAGIC
# MAGIC     Notebook: Employee_department-spark-session
# MAGIC               Read teh CSV
# MAGIC               Fix any datatype discrepancies
# MAGIC               Load the data from the csv to each of the tables previously created  
# MAGIC               Query those tables
# MAGIC
# MAGIC 6 Upload the data from those csv files to each of teh tables previously created customer/orders/products/returns
# MAGIC
# MAGIC     Notebook: customers_products_orders_returns-spark-session
# MAGIC               Read teh CSV
# MAGIC               Fix any datatype discrepancies
# MAGIC               Load the data from the csv to each of the tables previously created   
# MAGIC               Query those tables
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

