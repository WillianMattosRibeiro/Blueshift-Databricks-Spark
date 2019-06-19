# Databricks notebook source
# MAGIC %sh wget https://raw.githubusercontent.com/erlacherDev/Blueshift-Databricks-Spark/master/Material%20de%20Apoio/Datasets/006%20-%20007/vgsales.csv
# MAGIC mv vgsales.csv /dbfs/FileStore/vgsales.csv

# COMMAND ----------

path = "FileStore/vgsales.csv"

gamesRDD = sc.textFile(path)

garbage = gamesRDD.first()

gamesWithoutGarbageRDD = gamesRDD.filter(lambda x: x != garbage)

header = gamesWithoutGarbageRDD.first()

gamesWithoutHeaderRDD = gamesWithoutGarbageRDD.filter(lambda x: x != header)

gamesSplitedRDD = gamesWithoutHeaderRDD.map(lambda x: x.split("|"))

# COMMAND ----------

(Name, Platform, Year, Publisher, Global_Sales) = range(5)

bestGamesRDD = gamesSplitedRDD.filter(lambda x: float(x[Global_Sales]) > 5.00)

bestGamesKeyByPublisherRDD = bestGamesRDD.keyBy(lambda x: x[Publisher])

# COMMAND ----------

countBestGamesByPublisherRDD = bestGamesKeyByPublisherRDD.map(lambda x: (x[0], 1)).reduceByKey(lambda x,y: x+y)

# COMMAND ----------

countSortedBestGamesByPublisherRDD = countBestGamesByPublisherRDD.sortBy(lambda x: float(x[1]), ascending=False)

# COMMAND ----------

rankedCountBestGamesByPublisher = countSortedBestGamesByPublisherRDD.zipWithIndex()\
.map(lambda x: (x[0][0], x[0][1], x[1]+1))

# COMMAND ----------

schema = "Publisher string, Count int, Rank int"

display(spark.createDataFrame(rankedCountBestGamesByPublisher, schema))

# COMMAND ----------

display(rankedCountBestGamesByPublisher.toDF(["Publisher", "Count", "Rank"]))
