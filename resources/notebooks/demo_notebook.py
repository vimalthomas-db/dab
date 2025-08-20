# Databricks notebook source
#range:
df = spark.range(10)


display(df)

# COMMAND ----------

collection = {
('1','vimal thomas'),
('2', 'anandi')

}

df1 = spark.createDataFrame(collection,['id','name'])

display(df1)
