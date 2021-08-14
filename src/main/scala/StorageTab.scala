import org.apache.spark.sql.SparkSession

object StorageTab extends App {

  val spark = SparkSession.builder
    .appName("Simple Application")
    .master("local")
    .getOrCreate()
  import org.apache.spark.storage.StorageLevel._

   val rdd = spark.sparkContext.range(0, 100, 1, 5).setName("rdd")

     rdd.persist(MEMORY_ONLY_SER)

       rdd.count

//       val df = Seq((1, "andy"), (2, "bob"), (2, "andy")).toDF("count", "name")
//
//       df.persist(DISK_ONLY)
//
//       df.count
}
