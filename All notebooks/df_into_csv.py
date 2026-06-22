# Databricks notebook source
data = [(1,'venkat',30000),(2,'karthi',35000),(3,'siva',28000)]
schema = ['id','name','salary']

df = spark.createDataFrame(data = data, schema = schema)
display(df)

# COMMAND ----------

df.write.csv(path='dbfs:/temp/emp',header=True, mode='overwrite' )

# COMMAND ----------

df = spark.read.csv(path='dbfs:/temp/emp',header=True)
display(df)

# COMMAND ----------


