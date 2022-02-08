import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

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
    val allTeams: Dataset[Row] = TransformData.getAllTeams(spark, path)

    println("## Import of dataset from CSV ## \n")
    ImportCSV.run(spark, path)
    println("Done. \n")

    println("## Data transformation ## \n")

    println("Select a Team")
    selectedTeam = readLine()

    var testSelectedTeam: Dataset[Row] = allTeams
      .filter(col("Teams")
        .contains(selectedTeam))

    var countSelectedTeam = testSelectedTeam.count()

    // Vérifie que l'équipe choisie est bien orthographié et dans la liste des équipe
    while (countSelectedTeam == 0){
      println("Select a correct team")
      allTeams.show()
      selectedTeam = readLine()

      var testSelectedTeam: Dataset[Row] = allTeams
        .filter(col("Teams")
          .contains(selectedTeam))

      countSelectedTeam = testSelectedTeam.count()
    }

    TransformData.statistics(spark, path, selectedTeam)

    println(s"$selectedTeam ratio : ${TransformData.getRatio(spark, path, selectedTeam)}")

    val totalVictory = TransformData.numberVictory(spark, path, selectedTeam)

    print(s"$selectedTeam win $totalVictory times in total")
  }
}
