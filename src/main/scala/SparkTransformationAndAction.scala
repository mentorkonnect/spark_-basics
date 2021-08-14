import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import org.apache.spark.storage.StorageLevel.{DISK_ONLY, MEMORY_ONLY}

object SparkTransformationAndAction extends App{
  val spark = SparkSession.builder
    .appName("Simple Application")
    .master("local")
    .getOrCreate()

//  spark.sparkContext.setLogLevel("ERROR")
  val urlfile="https://covidtracking.com/api/states/daily.csv"
  spark.sparkContext.addFile(urlfile)
  val df = spark.read.option("inferSchema", false).option("header", true).csv("file:///"+SparkFiles.get("daily.csv"))
//  df.persist(MEMORY_ONLY)

  // TRANSFORMATIONS
  val selected = df.select("date","state","positive")
  val highly_critical = selected.filter("positive > 100000")
  val sorted_desc = highly_critical.sort(desc("positive"))

  // ACTIONS
  val h_critical = highly_critical.count
  val total = selected.count
  println("critical count: "+h_critical)
  println("total count: "+total)
  highly_critical.show
  sorted_desc.show

  Thread.sleep(100000)

}
