import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, max}

object TransformData {

  /*
   * Function to obtain the 5 best teams
   */
  def getBestTeams(sparkSession: SparkSession, path: String): Dataset[Row] ={

    val allSeason = ImportCSV.run(sparkSession, path)

    val bestTeams = allSeason
      .filter(col("FTHG") - col("FTAG") >= 4)
      .filter(col("HTHG") - col("HTAG") > 1)
      .filter(col("HTR").equalTo("H"))

    bestTeams.printSchema()
    bestTeams.show()
    bestTeams
  }
}
