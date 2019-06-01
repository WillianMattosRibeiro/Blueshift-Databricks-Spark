# Agrupar cada palavra da musica por sua letra inicial

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("First Letter Ap").getOrCreate()

path = "D:\\PyCharm\\Blueshift-DatabricksSpark\\Material de Apoio\\Datasets\\florentina.txt"

florentinaRDD = spark.sparkContext.textFile(path)

wordsRDD = florentinaRDD.flatMap(lambda x: x.split(" "))

firstletterRDD = wordsRDD.keyBy(lambda x: x[0].lower())

groupedRDD = firstletterRDD.groupByKey().mapValues(list)

groupedRDD.take(1)

groupedRDD.toDebugString()