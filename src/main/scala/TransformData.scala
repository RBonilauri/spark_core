import org.apache.spark
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

import scala.collection.mutable.ListBuffer

object TransformData {

  /*
   * Function that get all seasons from ImportCSV
   * Return a dataset of all seasons
   */
  def getAllSeasons(sparkSession: SparkSession, path: String): Dataset[Row] = {
    val allSeason = ImportCSV.run(sparkSession, path)

    allSeason
  }

  /*
   * Function that creates a list with the names of the teams
   * Return a list of teams
   */
  def getAllTeams(sparkSession: SparkSession, path: String): Dataset[Row] = {

    val allSeason = getAllSeasons(sparkSession, path)

    val allTeams = allSeason.select("AwayTeam")
      .distinct()
      .withColumnRenamed("AwayTeam", "Teams")

    allTeams
  }

  /*
   * Function to get statistics
   * Take the name of a team and gets the average of goals scored and conceded per game
   */
  def statistics(sparkSession: SparkSession, path: String, teamName: String): Unit = {

    println(s"======================== Average for $teamName ======================== \n")

    val allSeason = getAllSeasons(sparkSession, path)

    // Creation of a dataset where HomeTeam = chosen team
    val homeTeamSelected = allSeason
      .filter(col("HomeTeam").equalTo(teamName))

    val numberOfLine = homeTeamSelected.count()

    // Get sum of FTHG & FTAG
    val totalGoalScored = homeTeamSelected
      .select(sum("FTHG"))

    val totalGoalConceded = homeTeamSelected
      .select(sum("FTAG"))

    // Get statistics
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

    // Get statistics
    val goalScoredAverageAway = awayTeamSelected
      .select(mean("FTAG"))

    val goalConcededAverageAway = awayTeamSelected
      .select(mean("FTHG"))

    println("- AWAY TEAM -")
    println(s"$teamName played $numberOfLineAway times between 2015 and 2019 as away Team")
    println(s"$teamName scored ${totalGoalScoredAway.head()} and conceded ${totalGoalConcededAway.head()} goals between 2015 and 2019")
    println(s"In average $teamName scored ${goalScoredAverageAway.head()} and conceded ${goalConcededAverageAway.head()} per match \n")

    // Get value of goalScoredAverage and goalConcededAverage for calculate the ratio
    val differenceAverage = goalScoredAverage.first().getDouble(0) - goalConcededAverage.first().getDouble(0)

    val differenceAverageAway = goalScoredAverageAway.first().getDouble(0) - goalConcededAverageAway.first().getDouble(0)

    println(s"Ratio home team : $differenceAverage")
    println(s"Ratio away team : $differenceAverageAway")
  }

  def getRatio(sparkSession: SparkSession, path: String, teamName: String): Double = {

    val allSeason = getAllSeasons(sparkSession, path)

    // HOME
    val goalScoredAverage = allSeason
      .filter(col("HomeTeam").equalTo(teamName))
      .select(mean("FTHG"))

    val goalConcededAverage = allSeason
      .filter(col("HomeTeam").equalTo(teamName))
      .select(mean("FTAG"))

    // AWAY
    val goalScoredAverageAway = allSeason
      .filter(col("AwayTeam").equalTo(teamName))
      .select(mean("FTAG"))

    val goalConcededAverageAway = allSeason
      .filter(col("awayTeam").equalTo(teamName))
      .select(mean("FTHG"))

    val differenceAverage = goalScoredAverage.first().getDouble(0) - goalConcededAverage.first().getDouble(0)

    val differenceAverageAway = goalScoredAverageAway.first().getDouble(0) - goalConcededAverageAway.first().getDouble(0)
    val averageRatio: Double = (differenceAverage + differenceAverageAway) / 2

    averageRatio
  }

  /*
   * Function to obtain the 5 best teams
   */
  def getBestTeams(sparkSession: SparkSession, path: String): Dataset[Row] = {

    println("======================== BEST TEAMS ========================")

    val allTeams = getAllTeams(sparkSession, path)

    val allTeamsWithRatio: Dataset[Row] = allTeams
      .withColumn("Ratio", col("Teams"))

//      .map(team => getRatio(sparkSession, path, team.toString()))
//      .withColumn("Ratio", col("Teams") as)

    allTeamsWithRatio
  }
}
