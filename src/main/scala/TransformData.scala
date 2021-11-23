import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object TransformData {

  def run(sparkSession: SparkSession, path: String): Unit ={

    val test = ImportCSV.run(sparkSession, path)

    val test2 = test
      .filter(col("HomeTeam").equalTo("Lille"))
      .filter(col("B365H").equalTo("2.1"))
      .filter(!col("AwayTeam").equalTo("Paris SG"))

    test2.show()
  }

}
