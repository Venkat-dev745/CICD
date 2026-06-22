# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# Initialize Spark session
spark = SparkSession.builder.appName("Create DataFrame").getOrCreate()

# Define the schema with data types for each column
schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity_sold", IntegerType(), True),
    StructField("cost_of_product", IntegerType(), True),
    StructField("sale_date", StringType(), True)
])

# Provided data
data = [
    (1, "P1", 2, 50, "2024-12-01"),
    (2, "P2", 5, 70, "2024-12-01"),
    (3, "P1", 3, 50, "2024-12-02"),
    (4, "P3", 4, 100, "2024-12-02"),
    (5, "P2", 1, 70, "2024-12-03"),
    (6, "P5", 5, 30, "2024-12-03"),
    (7, "P1", 3, 50, "2024-12-03"),
    (8, "P2", 1, 70, "2024-12-04"),
    (9, "P4", 4, 200, "2024-12-04"),
    (10, "P5", 2, 30, "2024-12-05"),
    (11, "P4", 4, 200, "2024-12-05"),
    (12, "P3", 4, 100, "2024-12-06"),
    (13, "P5", 4, 30, "2024-12-07")
]

# Create DataFrame with schema
sales_df = spark.createDataFrame(data, schema)

# Show the data and schema
sales_df.printSchema()
sales_df.show()


# COMMAND ----------

sales_df = sales_df.withColumn("sale_amount", (col("quantity_sold") * col("cost_of_product")).cast(IntegerType()))
sales_df.show()

# COMMAND ----------

from pyspark.sql.functions import col, to_date,sum,desc,count,aggregate,lag,lead,when,dayofweek
sales_df = sales_df.withColumn("sale_date",to_date(col("sale_date"),"yyyy-MM-dd"))
sales_df.printSchema()

# COMMAND ----------

# 1: Total sales amount and quantity sold for each product
aggrigated_df = sales_df.groupBy("product_id").agg(sum("sale_amount").alias("total_sales"), sum("quantity_sold").alias("total_quantity"))
aggrigated_df.show()

# COMMAND ----------

# 2. Product with the highest sales amount

highest_sales = sales_df.groupBy("product_id").agg(sum("sale_amount").alias("total_sale_amount")).orderBy(desc("total_sale_amount")).limit(1)
highest_sales.show()

# COMMAND ----------

# Step 3: Filter transactions for a specific date range (e.g., "2024-12-01" to "2024-12-02")
print("Filtered Transactions (Date Range 2024-12-01 to 2024-12-04):")
date_range_df = sales_df.filter((col("sale_date") >= "2024-12-01") & (col("sale_date") <= "2024-12-03"))
date_range_df.show()

# COMMAND ----------

# Step 4: Display the top 3 products by total sales
top3_sales_df = aggrigated_df.orderBy(desc(col("total_sales"))).limit(3)
top3_sales_df.show()

# COMMAND ----------

# Step 5: Calculate daily total sales for each product
daily_total_sales_per_product = sales_df.groupBy("product_id", "sale_date").agg(sum("sale_amount").alias("total_sales"))
daily_total_sales_per_product.show()

# COMMAND ----------

# Step 6: Calculate the percentage contribution of each product to total sales
# collect can be used for assigned as variable and and used for others

total_sales_amount = sales_df.agg(sum("sale_amount").alias("total_sales_amount")).collect()[0]["total_sales_amount"]
aggrigated_df = aggrigated_df.withColumn("percetage_contribution_by_product", (col("total_sales")/total_sales_amount * 100).cast("decimal(5,2)"))
aggrigated_df.show()

# COMMAND ----------

# Step 7: Calculate cumulative sales amount over days for each product
from pyspark.sql.window import Window
windowSpec = Window.partitionBy("product_id").orderBy("sale_date")
cumulative_sale_df = daily_total_sales_per_product.withColumn("cumulative_sales", sum("total_sales").over(windowSpec))
cumulative_sale_df.show()

# COMMAND ----------

# show all previous total sales

daily_sales_with_previous_df = daily_total_sales_per_product \
    .withColumn("previous_total_sales", lag("total_sales").over(windowSpec))
daily_sales_with_previous_df.show()

# COMMAND ----------

# Step 8: Calculate the daily growth rate for each product
from pyspark.sql.functions import lag,col,when,rank
daily_sales_with_growth_df = daily_sales_with_previous_df.withColumn("growth_rate", when(col("previous_total_sales").isNotNull(),((col("total_sales") - col("previous_total_sales")) / col("previous_total_sales") * 100)).otherwise(None).cast("decimal(5,2)"))
daily_sales_with_growth_df.show()

# COMMAND ----------

# Step 9: Identify the top product for each day

top_product_per_each_day = sales_df.withColumn("rank", rank().over(Window.partitionBy("sale_date").orderBy(desc("sale_amount")))).filter(col("rank") == 1)
top_product_per_each_day.show()

# COMMAND ----------

# Step 10: Identify top 20% products based on total sales
sorted_df = aggrigated_df.orderBy(desc("total_sales"))
top50_percent = int(0.5 * sorted_df.count())
top50_percent_products = sorted_df.limit(top50_percent)
top50_percent_products.show()


# COMMAND ----------

# Step 4: Seasonal Trend - Daily Sales Analysis (Weekdays vs. Weekends)
from pyspark.sql.functions import dayofweek
sales_day_of_week_df = sales_df.withColumn("day_of_week", dayofweek(col("sale_date")))\
    .withColumn("is_weekend", when(col("day_of_week").isin(1,7),1).otherwise(0))
sales_day_of_week_df.show()


# COMMAND ----------

summary_df = sales_day_of_week_df.groupBy(col("is_weekend")).agg(count("transaction_id").alias("no_of_transactions"),
                                                                 sum("quantity_sold").alias("stock_purchased"),
                                                                 sum("sale_amount").alias("revenue"))
summary_df.show()


# COMMAND ----------

data = [
    (1, "2024-12-01", "Present"),
    (2, "2024-12-01", "Absent"),
    (1, "2024-12-02", "Absent"),
    (2, "2024-12-02", "On Leave"),
    (1, "2024-12-03", "Present"),
    (2, "2024-12-03", "Absent"),
    (3, "2024-12-01", "Present"),
    (3, "2024-12-02", "Present"),
    (3, "2024-12-03", "On Leave")
]
columns = ["employee_id", "date", "status"]

# Create DataFrame
attendance_df = spark.createDataFrame(data, columns)
attendance_df.show()

# COMMAND ----------

# Step 1: Calculate the total number of days per status for each employee
status_count_df = attendance_df.groupBy("employee_id", "status").agg(count("*").alias("stutus_count")).orderBy("employee_id")
status_count_df.show()

# COMMAND ----------

# Step 2: Identify employees with more than 2 absent days
absent_df = status_count_df.filter((col("status") == "Absent") & (col("stutus_count") > 1))
absent_df.show()

# COMMAND ----------


