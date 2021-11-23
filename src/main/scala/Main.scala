import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Main {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkProject")
      .master(master = "local[*]")
      .getOrCreate()

    val path = "C:/Users/33660/Documents/ESGI_4A/Spark_core/Projet/data/"

    println("Import of dataset from CSV... \n")
    ImportCSV.run(spark, path)
    println("Done. \n")

    println("Data transformation... \n")
    TransformData.getBestTeams(spark, path)
    println("Done. \n")
  }
}
