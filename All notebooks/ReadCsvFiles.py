# Databricks notebook source
df = spark.read.csv(path=['dbfs:/FileStore/products.csv','dbfs:/FileStore/customer.csv'],header=True,inferSchema=True)
display(df)
df.printSchema()

# COMMAND ----------

df.cache()

# COMMAND ----------

df.count()

# COMMAND ----------

df.repartition(10)

# COMMAND ----------

df_transactions = spark.read.csv(path='dbfs:/FileStore/transactions.csv',header=True,inferSchema=True)
display(df_transactions)


# COMMAND ----------

df_transactions1 = df_transactions.select("transaction_id").distinct()
display(df_transactions1)

# COMMAND ----------

help(spark.read.csv)

# COMMAND ----------

df = spark.read.csv(path='dbfs:/FileStore/',header=True) 
display(df)
df.printSchema()
df.count()

# COMMAND ----------

from pyspark.sql.types import *
help(StructType)

# COMMAND ----------

from pyspark.sql.types import *
schema = StructType([StructField('product_id',IntegerType()),StructField('customer_id',IntegerType())])
df = spark.read.csv(path=['dbfs:/FileStore/products.csv','dbfs:/FileStore/customer.csv'],schema = schema, header=True)
display(df)
df.printSchema()

# COMMAND ----------

from pyspark.sql.types import *
schema = StructType().add(field='customer_id',data_type=IntegerType())\
                     .add(field ='product_id',data_type=IntegerType())
df = spark.read.csv(path=['dbfs:/FileStore/products.csv','dbfs:/FileStore/customer.csv'],schema = schema, header=True)
display(df)
df.printSchema()

# COMMAND ----------

df_customer = spark.read.csv(path='dbfs:/FileStore/customer.csv',header=True,inferSchema=True)
display(df_customer)


# COMMAND ----------


