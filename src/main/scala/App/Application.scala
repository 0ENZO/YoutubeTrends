package App

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import readers.{CsvReader, JsonReader}


object Application {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("TP1")
      .master("local[*]")
      .getOrCreate()

    val df = new CsvReader(spark).read("data/CAvideos.csv")
    val df2 = new JsonReader(spark).read("data/CA_category_id.json")

  }
}
