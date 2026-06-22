# Databricks notebook source
data = [1,2,3,4,5,6,7,8,9,10,11,12]
rdd=spark.sparkContext.parallelize(data)  # no of partitions = 3, if we don't mension automatically partitioned
rddCollect = rdd.collect()
print(rddCollect)
print("Number of Partitions: "+str(rdd.getNumPartitions()))
print("Action: First element: "+str(rdd.first()))


# COMMAND ----------

print(rdd.repartition(2).collect())
print("Number of Partitions: "+str(rdd.getNumPartitions()))

# COMMAND ----------

rdd1 = spark.sparkContext.textFile('dbfs:/FileStore/products.csv')
print(rdd1.collect())

# COMMAND ----------

# rdd transformations :- 

rdd2 = rdd1.flatMap(lambda x: x.split(" "))
print(rdd2.collect())

# COMMAND ----------

# map :-

rdd3 = rdd2.map(lambda x: (x,1))
print(rdd3.collect())

# COMMAND ----------

# reduceByKey :- 
rdd4 = rdd3.reduceByKey(lambda a,b: a+b)

# COMMAND ----------

# sortByKey :-

rdd5 = rdd4.map(lambda x: (x[1],x[0])).sortByKey()
#Print rdd5 result to console
print(rdd5.collect())

# COMMAND ----------

# filter :- filtering all words starts with “a”.

rdd4 = rdd3.filter(lambda x : 'an' in x[1])
print(rdd4.collect())

# COMMAND ----------

# rdd actions :-
# count :- Returns the number of records in an RDD
print("Count : "+str(rdd1.count()))

# COMMAND ----------

# first :-  Returns the first record.

firstRec = rdd1.first()
print("First Record : "+str(firstRec[0]) + ","+ firstRec[1])

# COMMAND ----------

# max :- Returns max record.

datMax = rdd1.max()
print("Max Record : "+str(datMax[0]) + ","+ datMax[1])

# COMMAND ----------

# reduce :- Reduces the records to single, we can use this to count or sum.

totalWordCount = rdd6.reduce(lambda a,b: (a[0]+b[0],a[1]))
print("dataReduce Record : "+str(totalWordCount[0]))

# COMMAND ----------

# Action - take :- Returns the record specified as an argument.

data3 = rdd6.take(3)
for f in data3:
    print("data3 Key:"+ str(f[0]) +", Value:"+f[1])

# COMMAND ----------

# Action - collect :- 
data = rdd6.collect()
for f in data:
    print("Key:"+ str(f[0]) +", Value:"+f[1])

# COMMAND ----------

# saveAsTextFile() – Using saveAsTestFile action, we can write the RDD to a text file.

rdd6.saveAsTextFile("/tmp/wordCount")
