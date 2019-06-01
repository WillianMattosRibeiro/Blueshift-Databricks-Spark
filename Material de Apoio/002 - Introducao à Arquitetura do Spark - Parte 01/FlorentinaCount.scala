import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.getOrCreate()

val florentina_path = "/FileStore/tables/florentina.txt"

val florentinaRDD = spark.sparkContext.textFile(florentina_path)

val wordFlorentinaRDD = florentinaRDD.flatMap(x => x.split(" "))

val filterFlorentinaRDD = wordFlorentinaRDD.filter(x =>  x.contains("Florentina"))

filterFlorentinaRDD.count()