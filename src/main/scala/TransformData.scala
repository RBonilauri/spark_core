import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col}

object TransformData {

  /*
   * Function to obtain the 5 best teams
   */
  def getBestTeams(sparkSession: SparkSession, path: String): Dataset[Row] = {

    val allSeason = ImportCSV.run(sparkSession, path)

    val bestTeams = allSeason
      .filter(col("FTHG") - col("FTAG") >= 4)
      .filter(col("HTHG") - col("HTAG") > 1)
      .filter(col("HTR").equalTo("H"))

    bestTeams.printSchema()
    bestTeams.show()
    bestTeams
  }

  /*
   * Function to get average
   * Take the name of a team and get average for each column
   */
  def average(sparkSession: SparkSession, path: String): Dataset[Row] = {

    val allSeason = ImportCSV.run(sparkSession, path)

    val getAverage: Dataset[Row] = allSeason
      .foreach(f => println(f))

    getAverage.show()
    getAverage
  }
}
