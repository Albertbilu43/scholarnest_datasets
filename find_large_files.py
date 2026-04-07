# Databricks notebook source
# MAGIC %md
# MAGIC ### Find Largae files in a dorectory
# MAGIC

# COMMAND ----------

import os

def find_large_files(directory, size_limit):
    large_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            if os.path.getsize(file_path) > size_limit:
                large_files.append(file_path)
    return large_files

if __name__ == "__main__":
    directory = '/Volumes/dev/spark_db/datasets/spark_programming/data/'
    size_limit = 10 * 1024 * 1024
    large_files = find_large_files(directory, size_limit)
    print("Large files found:")
    for file in large_files:
        print(file)

# COMMAND ----------

