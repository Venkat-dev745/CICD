# Databricks notebook source
# date functions :- 

from pyspark.sql.functions import *

data=[("1","2020-02-01"),("2","2019-03-01"),("3","2021-03-01")]
df1=spark.createDataFrame(data,["id","input"])
df1.show()


# COMMAND ----------

# 1) current_date() :- 

df1 = df1.select(current_date().alias('current_date')).show()

# COMMAND ----------

# 2) date_format()

df1.select("input", date_format("input", "dd-MM-yyyy").alias("date_format")).show()


# COMMAND ----------

# 3) to_date :- converts string to date type

df1.select('input', to_date('input', 'yyyy-MM-dd').alias('to_date')).show()

# COMMAND ----------

# 4)datediff :- 

df1.select('input', current_date(), datediff(current_date(), 'input').alias('datediff')).show()

# COMMAND ----------

# 5) months_between() :- The below example returns the months between two dates using months_between().

df1.select('input', current_date(), round(months_between(current_date(), 'input'), 2).alias('months_between')).show()

# COMMAND ----------

# 6) trunc() :- The below example truncates the date at a specified unit using trunc().


#trunc()
df1.select(col("input"), 
    trunc(col("input"),"Month").alias("Month_Trunc"), 
    trunc(col("input"),"Year").alias("Month_Year"), 
    trunc(col("input"),"day").alias("Month_Day")
   ).show()

# COMMAND ----------

# 7) add_months() , date_add(), date_sub()

df1.select('input', 
    add_months('input', 3).alias('add_months'),
    add_months('input', -3).alias('sub_months'),
    date_add('input', 4).alias('date_add'),
    date_sub('input', 4).alias('date_sub')
  ).show()

# COMMAND ----------

# year(), month(), next_day(), weekofyear() :- 


df1.select("input", 
     year("input").alias("year"), 
     month("input").alias("month"), 
     next_day("input", "Sunday").alias("next_day"),
     weekofyear("input").alias("weekofyear")
     
  ).show()


# COMMAND ----------

# dayofweek(), dayofmonth(), dayofyear()

df1.select("input",  
     dayofweek("input").alias("dayofweek"), 
     dayofmonth("input").alias("dayofmonth"), 
     dayofyear("input").alias("dayofyear"), 
  ).show()

# COMMAND ----------

# timestamp functions :-

data=[["1","02-01-2020 11 01 19 06"],["2","03-01-2019 12 01 19 406"],["3","03-01-2021 12 01 19 406"]]
df2=spark.createDataFrame(data,["id","input"])
df2.show(truncate=False)

# COMMAND ----------

# current_timestamp()

df2.select(current_timestamp().alias('current_timestamp')).show(1, truncate = False)

# COMMAND ----------

# to_timestamp()

df2.select("input", 
    to_timestamp("input", "MM-dd-yyyy HH mm ss SSS").alias("to_timestamp") 
  ).show(truncate=False)

# COMMAND ----------

# hour(), Minute() and second()

data=[["1","2020-02-01 11:01:19.06"],["2","2019-03-01 12:01:19.406"],["3","2021-03-01 12:01:19.406"]]
df3=spark.createDataFrame(data,["id","input"])

df3.select(col("input"), 
    hour(col("input")).alias("hour"), 
    minute(col("input")).alias("minute"),
    second(col("input")).alias("second") 
  ).show(truncate=False)


# COMMAND ----------


