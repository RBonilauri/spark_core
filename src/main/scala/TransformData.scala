import org.apache.spark
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object TransformData {

  /*
   * Function that creates a list with the names of the teams
   */
  def getAllTeams(sparkSession: SparkSession, path: String): List[Any] = {

    val allSeason = ImportCSV.run(sparkSession, path)

    val allTeams = allSeason.select("AwayTeam").collect().map(_(0)).toList
    allTeams
  }

  /*
   * Function to get average
   * Take the name of a team and gets the average of goals scored and conceded per game
   */
  def average(sparkSession: SparkSession, path: String, teamName: String): Unit = {

    println(s"======================== Average for $teamName ======================== \n")

    val allSeason = ImportCSV.run(sparkSession, path)

    /************************* FOR HOME TEAM *******************************/

    val homeTeamSelected = allSeason
      .filter(col("HomeTeam").equalTo(teamName))

    val numberOfLine = homeTeamSelected.count()

    println("- HOME TEAM -")
    println(s"$teamName played $numberOfLine times between 2015 and 2019 as home Team")

    // Get sum of FTHG & FTAG
    val totalGoalScored = homeTeamSelected
      .select(sum("FTHG"))

    val totalGoalConceded = homeTeamSelected
      .select(sum("FTAG"))

    println(s"$teamName scored ${totalGoalScored.head()} and conceded ${totalGoalConceded.head()} goals between 2015 and 2019")

    // Get average
    val goalScoredAverage = homeTeamSelected
      .select(mean("FTHG"))

    val goalConcededAverage = homeTeamSelected
      .select(mean("FTAG"))

    println(s"In average $teamName scored ${goalScoredAverage.head()} and conceded ${goalConcededAverage.head()} per match \n")

    /************************* FOR AWAY TEAM *******************************/

    val homeTeamSelectedAway = allSeason
      .filter(col("AwayTeam").equalTo(teamName))

    val numberOfLineAway = homeTeamSelectedAway.count()

    println("- AWAY TEAM -")
    println(s"$teamName played $numberOfLine times between 2015 and 2019 as home Team")

    // Get sum of FTHG & FTAG
    val totalGoalScoredAway = homeTeamSelectedAway
      .select(sum("FTAG"))

    val totalGoalConcededAway = homeTeamSelectedAway
      .select(sum("FTHG"))

    println(s"$teamName scored ${totalGoalScoredAway.head()} and conceded ${totalGoalConcededAway.head()} goals between 2015 and 2019")

    // Get average
    val goalScoredAverageAway = homeTeamSelectedAway
      .select(mean("FTAG"))

    val goalConcededAverageAway = homeTeamSelectedAway
      .select(mean("FTHG"))

    println(s"In average $teamName scored ${goalScoredAverageAway.head()} and conceded ${goalConcededAverageAway.head()} per match")
  }

  /*
   * Function to obtain the 5 best teams
   */
  def getBestTeams(sparkSession: SparkSession, path: String): Dataset[Row] = {

    println("======================== BEST TEAMS ========================")

    val allSeason = ImportCSV.run(sparkSession, path)

    val bestTeams = allSeason
      .filter(col("FTHG") - col("FTAG") >= 4)
      .filter(col("HTHG") - col("HTAG") > 1)
      .filter(col("HTR").equalTo("H"))


    bestTeams.show()
    bestTeams
  }
}
