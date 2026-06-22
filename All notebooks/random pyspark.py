# Databricks notebook source
from pyspark.sql.functions import *
data = [("venkat", 20000, 26), ("ram", 34000, 28), ("ashok", 23000, 34)]
schema = ["name", "salary", "age"]
dataframe = spark.createDataFrame(data, schema)
dataframe.show()

# COMMAND ----------

dataframe.cache()
dataframe.count()

# COMMAND ----------

dataframe.unpersist()

# COMMAND ----------

from pyspark import StorageLevel
dataframe.persist(StorageLevel.MEMORY_AND_DISK)
dataframe.persist(StorageLevel.MEMORY_ONLY)
dataframe.unpersist()

# COMMAND ----------

df1 = dataframe.orderBy(col("age").desc()).show()

# COMMAND ----------



# COMMAND ----------

df = spark.read.format("csv").option("header", "true").load("abfss//:container@storageaccount.dfs.core.wndows.net/foldername.data.csv")
df.show()

# COMMAND ----------

# lazy evalution - in pyspark tranformation are lazy, means they wont executed immediately, they only triggerd once the action perfomed, actions are show, count, collect

df = dataframe.filter(col("age") > 30)
df = df.where("age > 30")
df.select("name","age")
df.show()

#optimizes query executions, by combining multiple tranformation into single stage

df.groupBy("id").agg(avg(col("salary")).alias("avg_sal"), count(col("age")).alias("no_of"))
df1.join(df2, df1.id == df2.id, "inner").show()
df1.join(df2, df1.id == df2.id, "outer").show()
df_union = df1.union(df2)



# COMMAND ----------

#dealing with null values

df.dropna()
df.fillna({"salary" : 0, "department" : "unknown"})
df.dropna(subnet=["name"])
df.dropna(subnet=["id"])
df.dropDuplicates()
df.dropDuplicates(["id", "name"])

# COMMAND ----------

# write to storage adls

df.write.format("csv").mode("overwrite").save("abfss://csvfiles@venkatblob.dfs.core.windows.net/logs/log.csv")
df.write.format("delta").mode("overwrite").save("abfss://csvfiles@venkatdatalake.dfs.core.windows.net/delta-tables/")


# COMMAND ----------

# convert from rdd to df
rdd = spark.sparkContext.parallelize([("venkat", 20), ("ram", 23)])
df = rdd.toDF(["name", "age"])
df.show()

# COMMAND ----------

df.filter((col("id") > 30) & (col("name") == "bob"))
df.select("name", "age")
df.withColumnRenamed("name", "names")
df.count()

# COMMAND ----------

# windows functions
from pyspark.sql.window import Window
windowSpec = Window.partitionBy('Id').orderBy(col('salary').desc())
df = df.withColumn("ranked", row_number().over(windowSpec))

df.groupBy("department").agg(avg("salary").alias("avg_sal"), sum("salary").alias("total")).orderBy("salary", ascending = False)

# COMMAND ----------

# optimization techniques
#partionBy

df.write.partitionBy("region").parquet("abfss://csvfiles@venkatdatalake.dfs.core.windows.net/partioned-data/")
df_partitioned = spark.read.parquet("abfss://csvfiles@venkatdatalake.dfs.core.windows.net/partioned-data")
df.filter(col("region") = "US").show()

# bucketing
df.write.format("parquet").bucketBy(3, "id").saveAsTable("Bucketed_data")
bucketed_df = spark.read.table("Bucketed_data")
bucketed_df.show()
# Let’s query the bucketed table to filter data for user_id = 1
bucketed_df.filter(bucketed_df.id == 1).show()

# COMMAND ----------

#create a new table using select
df.select("name", "age").write.format("parquet").saveASTable()
#multiple joins
df1.join(df2, "id").join(df3, "id")
# find duplicates
df.groupBy(id).count().filter(col("count") > 1)
# selecting multiple columns while joining
df1.join(df2, df1.id == df2.id, "inner").select(df1.name,df2.name)
# when then assign a value
df.select(when(col("name") = "venkat", "good").when(col("satheesh"), "bad").otherwise("gudbad"))

df.filter(df.name.isin("venkat", "ram"))

df.filter(~df.name.isin("venkat", "venkey"))

df.filter(df.name.isNull())

df.filter(df.name.isNotNull())

df.select(datediff("end_date", "start_date"))

add_months()

df.withColumn("add words", contat_ws(col1, col2))

df.distinct()

df.select(round(col1, 2))

df.select(current_date())

df.select(date_sub(col1, 4))

df.select(month(col1))

df.select(year(col1))

#group by multiple columns
df.groupBy("id", name)

# Predicate Pushdown

df = spark.read.fomat("parquet").load(path).filter(col("region") > "India")

df.replace({"status": {"old" : "legacy", "new":"current"}})

df.drop("hc") # remove hc column

df.select("id").distinct()

coalesce() - return first non null values

df.withColumn("year", substring(col("date"), 1, 4))

to_date()

datediff()

% run notebook path - to run another notebook

pivot() - which can tranpose one column into many column

df.write.format("parquet").mode("overwrite").save("path")

df.write.partitionBy("year").parquet("path")

df.write.bucketBy(10, "id").format("parquet").mode("overwrite").saveAsTable("path")

need group by category amount > 1000

df.filter(col("amount") > 1000).groupBy("category").sum().show()



# COMMAND ----------

# MAGIC %sql
# MAGIC create table sales_delta
# MAGIC using delta
# MAGIC location "abfss://c@c.dfs.core.windows.net/sales_data"
# MAGIC
# MAGIC
# MAGIC select * from delta."abfss://c@c.dfs.core.windows.net/sales_data""
# MAGIC
# MAGIC merge into delta."location" as target
# MAGIC using delta."location" as source
# MAGIC on target.id = source.is
# MAGIC when matched then
# MAGIC   update set target.amount = source.amount
# MAGIC when not matched then
# MAGIC   insert into (id, category, amount) values(source.id, source.category, source.amount)
# MAGIC
# MAGIC
# MAGIC optimize delta."path"
# MAGIC zorder by category;
# MAGIC
# MAGIC describe history delta."location"
# MAGIC
# MAGIC select * from delta."location"
# MAGIC version as of 3
# MAGIC
# MAGIC restore table delta."location"
# MAGIC to version as of 10
# MAGIC
# MAGIC vacuum delta."location"
# MAGIC retain 168 hours

# COMMAND ----------

# scd1 

dim_customer = spark.createDataFrame([
    (1, "Alice", "NY"),
    (2, "Bob", "LA"),
    (3, "Charlie", "SF")
], ["customer_id", "name", "location"])

# Incoming Updated Data (Source)
incoming_data = spark.createDataFrame([
    (1, "Alice", "Texas"),  # Updated location
    (2, "Bob", "LA"),       # No change
    (3, "Charlie", "Chicago")  # Updated location
], ["customer_id", "name", "location"])

# Perform an SCD Type 1 Update (Overwrite)
updated_dim = incoming_data.alias("src") \
    .join(dim_customer.alias("tgt"), "customer_id", "outer") \
    .select(
        col("src.customer_id"),
        col("src.name"),
        col("src.location")
    )

# Show Updated Dimension Table
updated_dim.show()

# COMMAND ----------

data = [
    ("mrf master", 2023, 1000),
    ("MRF-Z", 2023, 2000),
    ("mrf-lite", 2024, 1500),
    ("apollo", 2023, 1800),
    ("goodyear", 2024, 1200),
    ("MRF", 2024, 2200)
]

df = spark.createDataFrame(data, ["brand", "year", "sales"])

df.show()

result_df = df.withColumn("brand_normalized", when(lower(col("brand")).like("%mrf%"), "MRF").otherwise(col("brand"))).groupBy("brand", "year").agg(sum("sales").alias("total_sales"))
result_df.show()

# COMMAND ----------

data = [hhhhhhhh
    ("MRF",2024,1000),
    ("MRF",2025,1500),
    ("Apollo",2024,2000),
    ("Apollo",2025,2500)
]
df = spark.createDataFrame(data, ["brand","year","sales"])
result = df.groupBy("brand").pivot("year").agg(sum("sales")).show()
