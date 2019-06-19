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

case class GamesRanked(
  publisher:String,
  Count:Integer,
  Rank:Long
)

val path = "FileStore/vgsales.csv"

val gamesRDD = sc.textFile(path)

val garbage = gamesRDD.first

val gamesWithoutGargabeRDD = gamesRDD.filter(x => x != garbage)

val header = gamesWithoutGargabeRDD.first

val gamesWithoutHeaderRDD = gamesWithoutGargabeRDD.filter(x => x != header)

val gamesSplitedRDD = gamesWithoutHeaderRDD.map(x => x.split("\\|")).map(x => Games( x(0), x(1), x(2), x(3), x(4).toFloat))

val bestGamesRDD = gamesSplitedRDD.filter(x => x.global_sales > 5.00)

val bestGamesKeyByPublisherRDD = bestGamesRDD.keyBy(x => x.publisher)

val countBestGamesByPublisherRDD = bestGamesKeyByPublisherRDD.map(x => ((x._1), 1)).reduceByKey(_+_)

val countSortedBestGamesByPublisherRDD = countBestGamesByPublisherRDD.sortBy(x => x._2, ascending=false)

val rankedCountBestGamesByPublisher = countSortedBestGamesByPublisherRDD.zipWithIndex()
.map(x => GamesRanked(
  (x._1)._1, (x._1)._2, x._2+1))

// COMMAND ----------

display(spark.createDataFrame(rankedCountBestGamesByPublisher))

// COMMAND ----------

display(rankedCountBestGamesByPublisher.toDF())
