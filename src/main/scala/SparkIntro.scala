import java.net.URL
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel.DISK_ONLY

object SparkIntro extends App{

  val spark = SparkSession.builder
    .appName("Simple Application")
    .master("local")
    .getOrCreate()

  val urlfile="https://covidtracking.com/api/states/daily.csv"
  spark.sparkContext.addFile(urlfile)

  val df = spark.read
    .option("inferSchema", true)
    .option("header", true)
//    .json("src/main/resources/finance.json")
    .csv( "file:///"+SparkFiles.get("daily.csv"))

  df.show
  df.printSchema()

  println(df.count())

  //  df.persist(DISK_ONLY)
  Thread.sleep(1000000)
}
