// Databricks notebook source
// MAGIC %sh wget https://raw.githubusercontent.com/erlacherDev/Blueshift-Databricks-Spark/master/Material%20de%20Apoio/Datasets/006%20-%20007/vgsales.csv
// MAGIC mv vgsales.csv /dbfs/FileStore/vgsales.csv

// COMMAND ----------

case class Games(
  name:String,
  platform:String,
  year:String,
  publisher:String,
  global_sales:Float
)

val path = "FileStore/vgsales.csv"

val gamesRDD = sc.textFile(path)

val garbage = gamesRDD.first

val gamesWithoutGargabeRDD = gamesRDD.filter(x => x != garbage)

val header = gamesWithoutGargabeRDD.first

val gamesWithoutHeaderRDD = gamesWithoutGargabeRDD.filter(x => x != header)

val gamesSplitedRDD = gamesWithoutHeaderRDD.map(x => x.split("\\|")).map(x => Games( x(0), x(1), x(2), x(3), x(4).toFloat))

val nintendoBestGamesRDD = gamesSplitedRDD.filter(x => {
  x.publisher == "Nintendo" & x.global_sales > 5})

val gamesSortedRDD = nintendoBestGamesRDD.sortBy(x =>  x.global_sales, ascending=false)

// COMMAND ----------

display(spark.createDataFrame(gamesSortedRDD))

// COMMAND ----------

display(gamesSortedRDD.toDF())
