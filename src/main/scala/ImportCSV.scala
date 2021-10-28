import org.apache.spark
import org.apache.spark.sql.SparkSession

object ImportCSV {

  val spark = SparkSession
    .builder()
    .appName("SparkProject")
    .master(master = "local[*]")
    .getOrCreate()

  val path = "C:/Users/33660/Documents/ESGI_4A/Spark_core/Projet/data/"

  val season_2018_2019 = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(path + "season-1819_csv.csv")

  val season_2017_2018 = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(path + "season-1718_csv.csv")

  val season_2016_2017 = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(path + "season-1617_csv.csv")

}
