import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import scala.Console.println
import scala.io.StdIn.readLine

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
    var selectedTeam: String = ""
    val allTeams = TransformData.getAllTeams(spark, path)

    println("## Import of dataset from CSV ## \n")
    ImportCSV.run(spark, path)
    println("Done. \n")

    println("## Data transformation ## \n")

    println("Select a Team")
    selectedTeam = readLine()

    // Vérifie que l'équipe choisie est bien orthographié et dans la liste des équipe
    while (!allTeams.contains(selectedTeam)){
      println("Select a correct team")
      println(allTeams)
      selectedTeam = readLine()
    }

    TransformData.statistics(spark, path, selectedTeam)

    println(s"$selectedTeam ratio : ${TransformData.getRatio(spark, path, selectedTeam)}")

    TransformData.getBestTeams(spark, path)
  }
}
