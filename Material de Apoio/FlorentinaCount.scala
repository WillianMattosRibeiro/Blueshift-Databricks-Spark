import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.getOrCreate()

val florentinaRDD = spark.sparkContext.textFile("/FileStore/tables/florentina.txt")

val wordFlorentinaRDD = florentinaRDD.flatMap(x => x.split(" "))

val filterFlorentinaRDD = wordFlorentinaRDD.filter(x =>  x.contains("Florentina"))

filterFlorentinaRDD.count()