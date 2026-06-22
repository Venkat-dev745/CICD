# Databricks notebook source
n = int(input())
if n > 1:
    for i in range(2, n):
        if n % i == 0:
            print("non-prime")
            break
    else:
        print("prime")
else:
    print("non-prime")



# COMMAND ----------

def factorial(n):
    if n < 1:
        return 1
    else:
        return n * factorial(n - 1)
print(factorial(int(input())))


# COMMAND ----------

def fibonacci(n):
    a, b = 0, 1
    for _ in range(n):
        print(a, end=" ")
        a, b = b, a + b

num = int(input("Enter the number of terms: "))
fibonacci(num)

# COMMAND ----------

def is_palindrome(s):
    return s == s[::-1]

s = input("Enter a string or number: ")
print("Palindrome" if is_palindrome(s) else "Not a Palindrome")

# COMMAND ----------

def is_leap_year(year):
    if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
        return "leap year"
    else:
        return "non-leap year"

year = int(input("Enter a year: "))
print(is_leap_year(year))


# COMMAND ----------

def is_armstrong(num):
    if sum(int(digit) ** len(str(num)) for digit in str(num)) == num:
        return "armstrong number"
    else:
        return "non-armstrong number"

num = int(input("Enter a number: "))
print(is_armstrong(num))


# COMMAND ----------

def is_perfect(n):
    if sum(i for i in range(1, n) if n % i == 0) == n:
        return True
    else:
        return False

num = int(input("Enter a number: "))
print("Perfect Number" if is_perfect(num) else "Not a Perfect Number")


# COMMAND ----------

from collections import Counter

sentence = "apple banana apple grape banana banana"
word_count = Counter(sentence.split())
print(dict(word_count))

# COMMAND ----------

df1 = df.withColumn("emp_name", replace(col("emp_name"), ""))
df.withColumn("department", (col("department_id").distict()).count().show())



