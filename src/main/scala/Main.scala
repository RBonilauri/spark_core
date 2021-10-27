import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {

    println("Hello world !")

    val spark = SparkSession
      .builder()
      .appName("SparkProject")
      .master(master = "local[*]")
      .getOrCreate()
  }
}
