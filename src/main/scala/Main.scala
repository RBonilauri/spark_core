import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object Main {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkProject")
      .master(master = "local[*]")
      .getOrCreate()

//    spark.sparkContext.setLogLevel("ERROR")

    val fthg1819 = ImportCSV.season_2018_2019
      .filter(col("FTHG") > 0)
//    fthg1819.show()

    val fthg1718 = ImportCSV.season_2017_2018
      .filter(col("FTHG") > 0)

    val test = fthg1819
      .filter(col("FTHG") === 4)
      .filter(col("FTAG") > 0)
//    test.show()
//    test.printSchema()

    val test2 = fthg1819
      .filter(col("FTHG") === col("FTAG"))
      .filter(col("HTR").equalTo("D"))
      .sort(col("FTHG").desc)
//    test2.show()

    val test3 = fthg1819
      .filter(col("HomeTeam").equalTo("Strasbourg"))
//      .filter(col("FTHG") === fthg1718.col("FTHG"))
//    test3.show()

    val psg1718 = ImportCSV.season_2017_2018
      .filter(col("HomeTeam").equalTo("Paris SG"))
      .filter(col("AwayTeam").equalTo("Marseille"))
      .select("Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG")

    val psg1819 = ImportCSV.season_2018_2019
      .filter(col("HomeTeam").equalTo("Paris SG"))
      .filter(col("AwayTeam").equalTo("Marseille"))
      .select("Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG")

    val psg1719 = psg1819
      .union(psg1718)
//    psg1719.show()

    val psg1617 = ImportCSV.season_2016_2017
      .filter(col("HomeTeam").equalTo("Paris SG"))
      .filter(col("AwayTeam").equalTo("Marseille"))
      .select("Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG")

    val psg1619 = psg1719
      .union(psg1617)
    psg1619.show()
  }
}
