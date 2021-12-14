import org.apache.spark
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object TransformData {

  /*
   * Function that creates a list with the names of the teams
   * Return a list of teams
   */
  def getAllTeams(sparkSession: SparkSession, path: String): List[Any] = {

    val allSeason = ImportCSV.run(sparkSession, path)

    val allTeams = allSeason.select("AwayTeam").collect().map(_(0)).toList
    allTeams
  }

  /*
   * Function to get average
   * Take the name of a team and gets the average of goals scored and conceded per game
   * Return the general ratio of goals
   */
  def average(sparkSession: SparkSession, path: String, teamName: String): Double = {

    println(s"======================== Average for $teamName ======================== \n")

    val allSeason = ImportCSV.run(sparkSession, path)

    // Creation of a dataset where HomeTeam = chosen team
    val homeTeamSelected = allSeason
      .filter(col("HomeTeam").equalTo(teamName))

    val numberOfLine = homeTeamSelected.count()

    // Get sum of FTHG & FTAG
    val totalGoalScored = homeTeamSelected
      .select(sum("FTHG"))

    val totalGoalConceded = homeTeamSelected
      .select(sum("FTAG"))

    // Get average
    val goalScoredAverage = homeTeamSelected
      .select(mean("FTHG"))

    val goalConcededAverage = homeTeamSelected
      .select(mean("FTAG"))

    println("- HOME TEAM -")
    println(s"$teamName played $numberOfLine times between 2015 and 2019 as home Team")
    println(s"$teamName scored ${totalGoalScored.head()} and conceded ${totalGoalConceded.head()} goals between 2015 and 2019")
    println(s"In average $teamName scored ${goalScoredAverage.head()} and conceded ${goalConcededAverage.head()} per match \n")

    // Creation of a dataset where AwayTeam = chosen team
    val awayTeamSelected = allSeason
      .filter(col("AwayTeam").equalTo(teamName))

    val numberOfLineAway = awayTeamSelected.count()

    // Get sum of FTHG & FTAG
    val totalGoalScoredAway = awayTeamSelected
      .select(sum("FTAG"))

    val totalGoalConcededAway = awayTeamSelected
      .select(sum("FTHG"))

    // Get average
    val goalScoredAverageAway = awayTeamSelected
      .select(mean("FTAG"))

    val goalConcededAverageAway = awayTeamSelected
      .select(mean("FTHG"))

    println("- AWAY TEAM -")
    println(s"$teamName played $numberOfLineAway times between 2015 and 2019 as home Team")
    println(s"$teamName scored ${totalGoalScoredAway.head()} and conceded ${totalGoalConcededAway.head()} goals between 2015 and 2019")
    println(s"In average $teamName scored ${goalScoredAverageAway.head()} and conceded ${goalConcededAverageAway.head()} per match \n")

    // Get value of goalScoredAverage and goalConcededAverage for calculate the ratio
    val differenceAverage = goalScoredAverage.first().getDouble(0) - goalConcededAverage.first().getDouble(0)

    val differenceAverageAway = goalScoredAverageAway.first().getDouble(0) - goalConcededAverageAway.first().getDouble(0)

    val averageRatio = (differenceAverage + differenceAverageAway) / 2

    println(s"Ratio home team : $differenceAverage & Ration away team : $differenceAverageAway")
    println(s"General ratio : $averageRatio \n\n")

    averageRatio
  }

  /*
   * Function to obtain the 5 best teams
   */
  def getBestTeams(sparkSession: SparkSession, path: String): Unit = {

    println("======================== BEST TEAMS ========================")

    val allSeason = ImportCSV.run(sparkSession, path)
  }
}
