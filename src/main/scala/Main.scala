import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, max}
import org.apache.log4j.Logger
import org.apache.log4j.Level

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

    val season_2018_2019 = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:/Users/33660/Documents/ESGI_4A/Spark_core/Projet/data/season-1819_csv.csv")

    val season_2017_2018 = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:/Users/33660/Documents/ESGI_4A/Spark_core/Projet/data/season-1718_csv.csv")


    val fthg1819 = season_2018_2019
      .filter(col("FTHG") > 0)
//    fthg1819.show()

    val fthg1718 = season_2017_2018
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

    val psg1718 = season_2017_2018
      .filter(col("HomeTeam").equalTo("Paris SG"))
      .filter(col("AwayTeam").equalTo("Marseille"))
      .select("Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG")

    val psg1819 = season_2018_2019
      .filter(col("HomeTeam").equalTo("Paris SG"))
      .filter(col("AwayTeam").equalTo("Marseille"))
      .select("Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG")

    val psg1719 = psg1819
      .union(psg1718)
    psg1719.show()


  }
}
