import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession

import scala.io.Source
import scala.io.Source.fromFile

object ReadSimpleCSV extends App{

  val bufferedSource = fromFile("src/main/resources/finance.csv")

  for (line <- bufferedSource.getLines) {
    val Array(month, revenue, expenses, profit) = line.split(",").map(_.trim)
    println(s"$month $revenue $expenses $profit")
  }
  bufferedSource.close
}


object ReadSparkCSV extends App{

  val spark = SparkSession.builder.appName("Simple Application")
    .master("local")
    .getOrCreate()

  val df = spark.read
    .option("inferSchema", true)
    .option("header", true)
    .csv("src/main/resources/finance.csv")
  df.show

}
