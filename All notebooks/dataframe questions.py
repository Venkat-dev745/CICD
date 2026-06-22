# Databricks notebook source
# 1. Total revenue per product
sales_df = df.withColumn("revenue", col("quantity") * col("price"))
sales_revenue = sales_df.groupBy("product").sum("revenue").alias("total_revenue")
sales_revenue.show()

# 2. Top 3 products with highest revenue
top3_products = revenue_df.ordeBy(col("total_revenue").desc()).limit(3)
top3_products.show()

# 3. Add transaction_month column
sales_df = sales_df.withColumn("transaction_month", date_format(to_date(col("transaction_date"), "dd-mm-yyyy"), "mm-yyyy"))
sales_df.show()

cust_txn_df = customer_df.join(transaction_df, on = "customer_id", how = "inner")
cust_txn_df.show()

# 4. Count transactions per customer
cust_txn_df = cust_txn_df.groupBy("customer_id").agg(count("*").alias("no_of_transactions")).show()
cust_txn_df.show()

# 5. Filter customers with >5 transactions
frequent_customer_df = cust_txn_df.filter(col("no_of_transactions") > 5)
frequent_customer_df.show()

# 6. Sort by transaction count descending
frequent_customer_df = frequent_customer_df.orderBy(col("no_of_transactions").desc())
frequent_customer_df.show()

# 7. join all the dataframes
sales_enriched_df = sales_df.join("customer_df", on = "customer_id", how = "inner")\
                            .join("product_df", on = "product_id", how = "inner")\
                            .withColumn("revenue", col("quantity") * col("price"))

# 8. Calculate total revenue per region & category
regional_category_revenue_df = sales_enriched_df.groupBy("region", "category").agg(sum(col("revenue")).alias("total_revenue"))
regional_category_revenue_df.show()

# 9. Top selling category per region
window_spec = Window.partitionBy("region").orderBy(col("total_revenue").desc())

top_category_df = regional_category_revenue_df.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1).drop("rank")

# 10. Find customers who purchased the same product more than once.

