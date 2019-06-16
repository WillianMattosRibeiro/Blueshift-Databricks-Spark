# Databricks notebook source
# MAGIC %sh wget https://raw.githubusercontent.com/erlacherDev/Blueshift-Databricks-Spark/master/Material%20de%20Apoio/Datasets/006%20-%20007/vgsales.csv
# MAGIC mv vgsales.csv /dbfs/FileStore/vgsales.csv

# COMMAND ----------

path = "FileStore/vgsales.csv"

gamesRDD = sc.textFile(path)

gamesRDD.take(10)

# COMMAND ----------

garbage = gamesRDD.first()

gamesWithoutGarbageRDD = gamesRDD.filter(lambda x: x != garbage)

gamesWithoutGarbageRDD.take(10)

# COMMAND ----------

header = gamesWithoutGarbageRDD.first()
gamesWithoutHeaderRDD = gamesWithoutGarbageRDD.filter(lambda x: x != header)

gamesWithoutHeaderRDD.take(10)

# COMMAND ----------

gamesSplitedRDD = gamesWithoutHeaderRDD.map(lambda x: x.split("|"))
gamesSplitedRDD.take(10)

# COMMAND ----------

nintendoBestGamesRDD = gamesSplitedRDD.filter(lambda x: (x[3] == "Nintendo") & (float(x[4]) > 5))
nintendoBestGamesRDD.take(10)

# COMMAND ----------

gamesSortedRDD = nintendoBestGamesRDD.map(lambda x: (x[0], x[1], x[2], x[3], float(x[4])))\
                       .sortBy(lambda x: x[4], ascending=False)\
                       

gamesSortedRDD.take(10)

# COMMAND ----------

schema = """
Name string,
Platform string,
Year string,
Publisher string,
Global_Sales float
"""
display(spark.createDataFrame(gamesSortedRDD, schema))

# COMMAND ----------

display(gamesSortedRDD.toDF(["Name","Platform","Year","Publisher","Global_Sales"]))
