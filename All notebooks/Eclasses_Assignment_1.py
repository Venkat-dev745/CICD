# Databricks notebook source
# 1.1 Create variables for the following types and print their values:

str_1 = input()
int_1 = int(input())
float_1 = float(input())
bool_1 = bool(input())
print(str_1)
print(int_1)
print(float_1)
print(bool_1)




# COMMAND ----------

a = int(input())
b = int(input())
c = float(input())
add_1 = a + b
print(add_1)
multi_1 = a * c
print(multi_1)
print(a > b)


# COMMAND ----------

str_1 = input()
str_2 = input()
concat_str = str_1 + " " + str_2
print(concat_str)

# COMMAND ----------

# 1.2 list operations

# Create a list of 5 different fruits. Print the list and its length.
str_2 = input()
list_1 = str_2.split(",")
print(list_1)
print(len(list_1))

# Perform the following operations on the list:

# Add a new fruit to the list
# Remove the second fruit from the list.
# Reverse the list.
# Sort the list alphabetically.
# Access and print the first and last fruit from the list.

list_1.append("pineapple")
list_1.pop(1)
list_1.reverse()
list_1.sort()
print(list_1)
first_fruit, last_fruit = list_1[0], list_1[-1]
print(first_fruit)
print(last_fruit)


# COMMAND ----------

# Create a tuple containing 4 different numbers.
input_1 = input()
tuple_1 = tuple(int(num) for num in input_1.split(","))
print(tuple_1)

# Access and print the second and third elements

sec_el = tuple_1[1]
third_el = tuple_1[2]
print(sec_el)
print(third_el)

# Try to modify the second element. What happens? (tuple is immutable object so elements in tuple cannot modified)
tuple_1[2] = 5
print(tuple_1)


# COMMAND ----------

# Find the maximum and minimum value in the tuple.
max_num, min_num = max(tuple_1), min(tuple_1)
print(max_num)
print(min_num)

# COMMAND ----------

# 1.4 Create a set containing 5 different colors.
input_2 = input()
set_1 = set(input_2.split(","))
print(set_1)

# COMMAND ----------

# Add a new color to the set.
set_1.add("brown")
print(set_1)
# Try adding a duplicate color. What happens? (it executes but if exists then can't add)
set_1.add("brown")
print(set_1)
# Remove a color from the set.
set_1.discard("white")
print(set_1)
# Check if a specific color exists in the set.
if "blue" in set_1:
    print("yes")
else:
    print("no")

# COMMAND ----------

# 1.5 Create a dictionary containing the following key-value pairs:

dict_1 = {
    "name": "John",
    "age": 25,
    "city": "New York",
    "profession": "Engineer"
}
print(dict_1)
# Add a new key-value pair to the dictionary for "salary."
dict_1["salary"] = 30000
# Update the value of the "age" key.
dict_1["name"] = "Venkat"
# Remove the "city" key-value pair.
del dict_1["city"]
print(dict_1)
# Access and print the "name" and "profession."
print(dict_1["name"])
print(dict_1["profession"])

# COMMAND ----------

# 1.6 Conditional Statements (if-else)
# Write a program that accepts a user's input for their age and prints:
age = int(input())
# "Child" if the age is less than 12
# "Teen" if the age is between 12 and 17
# "Adult" if the age is 18 or more

if age < 12:
    print("child")
elif age >= 12 and age <= 17:
    print("Teen")
else:
    print("Adult")


# COMMAND ----------

# Write a program that checks if a number is even or odd.
a = int(input())
if a % 2 == 0:
    print("even")
else:
    print("odd")

# COMMAND ----------

# 1.7  For Loop

# Create a list of numbers from 1 to 10 and use a for loop to:
# Print each number
n1, n2 = 1, 10
list_a = []
for i in range(n1, n2+1):
    list_a.append(i)
print(list_a)

# Print the square of each number
list_a1 = [i ** 2 for i in list_a]
print(list_a1)

# Write a program that uses a for loop to print the multiplication table of 5 (from 1 to 10).
n = int(input())
for i in range(1, 11):
    print(n, "*", i, "=", n*i)


# COMMAND ----------

# 1.8: While Loop
# Write a while loop that prints numbers from 1 to 5.
i = 1
n = 5
while i < n + 1:
    print(i, end=" ")
    i = i + 1
print(end="\n")
# Write a program that uses a while loop to find the sum of all numbers from 1 to 100.
a = 1
b = 100
total = 0
while a < b + 1:
    total += a
    a += 1
print(total)


# COMMAND ----------

# 1.9: User-Defined Functions
# Write a function greet_user(name) that accepts a user’s name as input and prints a greeting message, e.g., "Hello, John!"
def greet_user(name):
    return "Hello, " + name + "!"
name = input()
print(greet_user(name))

# Write a function calculate_sum(a, b) that returns the sum of two numbers.
def calculate_sum(a, b):
    return a + b
a = int(input())
b = int(input())
print(calculate_sum(a, b))

# Write a function is_even(n) that checks if a number is even. Return True if it is, otherwise return False.
def is_even(n):
    if n % 2 == 0:
        return True
    else:
        return False
n = int(input())
print(is_even(n))

# COMMAND ----------

# Create a DataFrame from a list of tuples with the columns: id, name, and age.
data = [(1, "venkat", 24), (2, "ram", 28), (3, "sam", 35)]
columns = ["id", "name", "age"]
df = spark.createDataFrame(data, columns)
display(df)

# COMMAND ----------

data1 = [(1, "venkat", "sales", 30000, "  Chennai", "working as a sales manager"), (2, "ram", "HR", 25000, "Bengaluru  ", "working as a HR"), (3, "sam", "Finance", 35000, " pune  ", "working as a finance analyst")]
columns1 = ["id", "name", "department", "salary", "location", "fashion"]
df1 = spark.createDataFrame(data1, columns1)
df1.show()
df1.printSchema()

# COMMAND ----------

df1.show(n=2)
df1.show(truncate = 20)

# COMMAND ----------

df2 = df1.select(df1.name, df1.salary) # df1.select("name", "salary") or df1.select(col("name"), col("salary"))
df2.show()

# COMMAND ----------

# string functions

from pyspark.sql.functions import upper, lower, concat, concat_ws, length, trim, lpad, rpad, regexp_replace, regexp_extract, lit, initcap, substring, substring_index, translate, regexp_extract, regexp_replace
df1.withColumn("deptu", upper(df1.department)).show()
df1.withColumn("deptl", lower(df1.department)).show()
df1.withColumn("length", length(df1.location)).show()
df1.withColumn("trim", trim(df1.location)).show()
df1.withColumn("lpad", lpad(df1.location, 10, ".")).show()
df1.withColumn("replace", regexp_replace(df1.salary, "0", "9")).show() # it can only replace one character not many
df1.withColumn("concat", concat(df1.name, lit(" works in the department of "), df1.department)).show(truncate = False)
df1.withColumn("concat_ws", concat_ws("-", df1.name, df1.department, df1.location)).show(truncate=False)
df1.withColumn("first_letter_cap", initcap(df1.name)).show()
df1.withColumn("first_3_chars", substring(df1.fashion, 1, 3)).show()
df1.withColumn("translate", translate(df1.fashion, "ae", "xz")).show()

# COMMAND ----------

data = [("john.doe@example.com",), ("alice.smith@domain.com",)]
df3 = spark.createDataFrame(data, ["email"])

# Extract domain name from email addresses
df3.withColumn("domain", regexp_extract("email", r"@([a-zA-Z0-9.-]+)", 1)).show()

# COMMAND ----------

df2 = df1.filter((df1.salary > 30000) & (df1.department == "Finance"))
df2.show()



# COMMAND ----------

import random
from datetime import timedelta, datetime
from pyspark.sql.functions import count, min, max, sum, avg, first, last
data = [
    (i,
     random.choice(["Electronics", "Furniture", "Clothing"]),
     random.choice(["Accessories", "Office Supplies", "Appliances"]),
     round(random.uniform(100, 5000), 2),
     random.randint(1, 20),
     round(random.uniform(0, 0.3), 2),
     datetime(2023, 1, 1) + timedelta(days=15 * i))
    for i in range(1, 21)
]
columns = ["ID", "Category", "Sub_Category", "Sales", "Quantity", "Discount", "Date"]
aggrigate_functions = spark.createDataFrame(data, columns)
aggrigate_functions.show()
aggrigate_functions.printSchema()

# COMMAND ----------

aggrigate_functions.groupBy("Category").agg(count("Category").alias("no of categories")).show()
aggrigate_functions.groupBy("Category").agg(sum("Sales")).show()
aggrigate_functions.groupBy("Category").agg(max("Sales")).show()
aggrigate_functions.groupBy("Category").agg(first("Quantity"), last("Quantity")).show()

# COMMAND ----------

# Windows_functions
from pyspark.sql.functions import rank, row_number, dense_rank, lead, lag, sum
from pyspark.sql.window import Window
data = [
    (1, "Electronics", 300),
    (2, "Electronics", 400),
    (3, "Furniture", 200),
    (4, "Furniture", 300),
    (5, "Electronics", 400),
    (6, "Furniture", 300),
    (7, "Electronics", 150),
    (8, "Furniture", 700)
]
columns = ["ID", "Category", "Sales"]
window_df = spark.createDataFrame(data, columns)
window_df.show()
window_df.printSchema()
windowSpec = Window.partitionBy("Category").orderBy("Sales")
window_df.withColumn("row_number", row_number().over(windowSpec))\
         .withColumn("rank", rank().over(windowSpec))\
         .withColumn("dense_rank", dense_rank().over(windowSpec))\
         .withColumn("cummulative_sales", sum("Sales").over(windowSpec))\
         .withColumn("previous_value", lag("Sales").over(windowSpec))\
         .withColumn("next_value", lead("Sales").over(windowSpec)).show()

# COMMAND ----------

from pyspark.sql.functions import array, explode, explode_outer

# Create an array column combining other columns
collection_fn_df = window_df.withColumn("value_array", array("Category", "Sales"))
collection_fn_df.drop("Category", "Sales").show()



# COMMAND ----------

data = [
    (1, ["apple", "banana"]),
    (2, []),               # Empty array
    (3, None),             # Null array
    (4, ["grape"])
]
columns = ["ID", "Fruits"]
collection_explodes_df = spark.createDataFrame(data, columns)
collection_explodes_df.select("ID", explode("Fruits").alias("Fruit")).show()

collection_explodes_df.select("id", explode_outer("Fruits").alias("Fruit")).show()
collection_explodes_df.withColumn("Fruit", explode_outer("Fruits")).show()


# COMMAND ----------

# conditional functions
from pyspark.sql.functions import when, col, coalesce
data = [
    ("Alice", 23),
    ("Bob", 17),
    ("Cathy", 45),
    ("David", 12),
    ("Eva", 34)
]
columns = ["Name", "Age"]
conditional_fn_df = spark.createDataFrame(data, columns)
conditional_fn_df.show()
conditional_fn_df.withColumn("category", when(col("age") < 18, "Minor")
                  .when((col("age") >= 18) & (col("age") < 30), "Young Adult")
                  .when((col("age") >= 30) & (col("age") < 50), "Adult")
                  .otherwise("Senior")
                  ).show()

# COMMAND ----------

data = [
    (1, None, "123-456-7890", "123 Main St"),
    (2, "alice@example.com", None, None),
    (3, None, None, "456 Maple Ave"),
    (4, None, None, None)
]
columns = ["ID", "Email", "Phone", "Address"]
coalesce_df = spark.createDataFrame(data, columns)
coalesce_df.withColumn("Alternate_contact", coalesce(col("Email"), col("Phone"), col("address"))).show()


# COMMAND ----------

# datefunctions

from pyspark.sql.functions import *
data = [
    (1, "2024-01-09", "2024-01-01", "2023-03-20", "2022-06-15"),
    (2, "2024-04-10", "2024-04-02", "2022-09-30", "2021-02-10"),
    (3, "2023-12-17", "2023-12-09", "2021-11-25", "2020-01-05"),
    (4, "2023-07-21", "2023-07-11", "2020-07-15", "2019-05-01")
]
columns = ["ID", "InvoiceDate", "OrderDate", "StartDate", "JoiningDate"]

# Convert date columns to date type
date_df = spark.createDataFrame(data, columns)
date_df.show()
date_df.printSchema()

new_date_df = date_df.withColumn("OrderDate", to_date(col("OrderDate"), "yyyy-MM-dd"))\
                     .withColumn("InvoiceDate", to_date(col("InvoiceDate"), "yyyy-MM-dd"))\
                     .withColumn("StartDate", to_date(col("StartDate"), "yyyy-MM-dd"))\
                     .withColumn("JoiningDate", to_date(col("JoiningDate"),"yyyy-MM-dd"))
new_date_df.show()

new_date_df.printSchema()


# COMMAND ----------

new_date_df = new_date_df.withColumn("current-date", current_date())\
           .withColumn("current_timestamp", current_timestamp())\
           .withColumn("days_since_order", datediff(current_date(), col("OrderDate")))\
           .withColumn("next_quarter", add_months(col("StartDate"), 3))\
           .withColumn("due_date", date_add(col("InvoiceDate"), 30))\
           .withColumn("grace_period_end", date_sub(col("due_date"), 5))\
           .withColumn("last_day_of_month", last_day("InvoiceDate"))\
           .withColumn("order_year", year(col("OrderDate")))\
           .withColumn("order_month", month(col("OrderDate")))\
           .withColumn("order_day", dayofmonth(col("OrderDate")))\
           .withColumn("OrderDayOfWeek", dayofweek(col("OrderDate")))\
           .withColumn("NoOfDaysPassedFromYearToOrder", dayofyear(col("OrderDate")))\
           .withColumn("MonthStart", date_trunc("month", col("Orderdate")))\
           .withColumn("YearStart", date_trunc("year", col("Orderdate")))\
           .withColumn("MonthsSinceJoining", round(months_between(current_date(),col("Joiningdate"))))\
           .withColumn("NextMonday", next_day(col("OrderDate"), "Mon")) \
           .withColumn("OrderQuarter", quarter(col("OrderDate"))) \
           .withColumn("OrderWeekOfYear", weekofyear(col("OrderDate")))\
           .withColumn("DayName", date_format(col("OrderDate"), "EEE"))\
           .withColumn("MonthName", date_format(col("OrderDate"), "MMM")).drop(col("ID"))
display(new_date_df)

# COMMAND ----------


data1 = [
    (1, "Alice", 3000),
    (2, "Bob", 4500),
    (3, "Charlie", 2500),
    (4, "David", 5000)
]
columns1 = ["CustomerID", "Name", "Spend"]

data2 = [
    (1, "NY"),
    (2, "LA"),
    (3, "SF"),
    (5, "Chicago")
]
columns2 = ["CustomerID", "City"]

df_joins1 = spark.createDataFrame(data1, columns1)
df_joins2 = spark.createDataFrame(data2, columns2)
df_joins1.show()
df_joins2.show()

df_inner = df_joins1.join(df_joins2, on = "CustomerID", how="inner")
df_inner.show()

df_left = df_joins1.join(df_joins2, on="CustomerID", how="left")
df_left.show()

df_right = df_joins1.join(df_joins2, on="CustomerID", how="right")
df_right.show()

df_outer = df_joins1.join(df_joins2, on="CustomerID", how="outer")
df_outer.show()

df_leftsemi = df_joins1.join(df_joins2, on="CustomerID", how="left_semi")
df_leftsemi.show()

df_leftanti = df_joins1.join(df_joins2, on="CustomerID", how="left_anti")
df_leftanti.show()

df_rightsemi = df_joins2.join(df_joins1, on="CustomerID", how="left_semi")
df_rightsemi.show()

df_rightanti = df_joins2.join(df_joins1, on="CustomerID", how="left_anti")
df_rightanti.show()

df_cross = df_joins1.crossJoin(df_joins2).select(df_joins1["CustomerID"], df_joins1["Name"], df_joins1["Spend"], df_joins2["City"])
df_cross.show()

# COMMAND ----------

# Perform a self join to find customers in the same city
data_customers = [
    (101, "Alice", "NY", 25),
    (102, "Bob", "LA", 30),
    (103, "Charlie", "NY", 35),
    (104, "Daisy", "SF", 28),
    (105, "Eva", "LA", 45)
]
columns_customers = ["CustomerID", "Name", "City", "Age"]

# Create DataFrame
df_customers = spark.createDataFrame(data_customers, columns_customers)
df_customers.show()

df_self_join = df_customers.alias("c1").join(df_customers.alias("c2"),(col("c1.City") == col("c2.City")) & (col("c1.CustomerID") < col("c2.CustomerID")),"inner")

# Select relevant columns to view the pairs
df_self_join.select(
    col("c1.CustomerID").alias("CustomerID1"),
    col("c1.Name").alias("Name1"),
    col("c1.City").alias("City"),
    col("c2.CustomerID").alias("CustomerID2"),
    col("c2.Name").alias("Name2")
).show()


# COMMAND ----------

# sql
data_orders = [
    (1, 101, "Laptop", 2),
    (2, 102, "Tablet", 1),
    (3, 101, "Smartphone", 1),
    (4, 103, "Laptop", 3),
    (5, 104, "Tablet", 2),
]
columns_orders = ["OrderID", "CustomerID", "Product", "Quantity"]

# Create the DataFrame
df_orders = spark.createDataFrame(data_orders, columns_orders)
df_orders.createOrReplaceTempView("Orders")
df_sql = spark.sql("select * from Orders where Product = 'Laptop'")
df_sql.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Orders where Product = "Laptop" and Quantity < 3
