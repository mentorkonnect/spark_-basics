import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY

object SparkCatalystOptimizerExample extends App{
  val spark = SparkSession.builder
    .appName("Simple Application")
    .master("local")
    .getOrCreate()
  val urlfile="https://covidtracking.com/api/states/daily.csv"
  spark.sparkContext.addFile(urlfile)
  val df = spark.read.option("inferSchema", true).option("header", true).csv("file:///"+SparkFiles.get("daily.csv"))
  df.persist(MEMORY_ONLY)

  // TRANSFORMATIONS
  val selected = df.select("date","state","positive")
  val highly_critical = selected.filter("positive > 100000")
  val sorted_desc = highly_critical.sort(desc("positive"))

  // ACTIONS
  val h_critical = highly_critical.count
  val total = selected.count

  sorted_desc.explain()

  val sorted_desc_sort_first = selected.sort(desc("positive"))
    .select("date","state","positive")
    .filter("positive > 100000")
  sorted_desc_sort_first.explain()

  highly_critical.show
  sorted_desc.show

  Thread.sleep(100000)

}
