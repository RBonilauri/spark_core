import org.apache.spark
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object ImportCSV {
  
  def run(sparkSession: SparkSession, path: String): Dataset[Row] = {

    val season_2018_2019 = sparkSession 
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path + "season-1819_csv.csv")

    val season_2017_2018 = sparkSession 
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path + "season-1718_csv.csv")

    val season_2016_2017 = sparkSession 
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path + "season-1617_csv.csv")

    val season_2015_2016 = sparkSession 
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path + "season-1516_csv.csv")

    val season_2015_2019 : Dataset[Row] = season_2015_2016
      .unionByName(season_2016_2017, true)
      .unionByName(season_2017_2018, true)
      .unionByName(season_2018_2019, true)

    season_2015_2019
  }
}
