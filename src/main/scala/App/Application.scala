package App

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import readers.{CsvReader, JsonReader}

import java.io.FileNotFoundException
import scala.sys.exit


object Application {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("TP1")
      .master("local[*]")
      .getOrCreate()

    val (dfCategories, dfVideos) = readFiles(spark)

    println("dfCategories")
    dfCategories.show(1, truncate = false)

    println("dfVideos")
    dfVideos.show(1, truncate = false)

    val requested_category = dfCategories.first().getAs[String]("id")
    println(s"requested categ : $requested_category")
    val videosOfCategory1 = dfVideos.filter(s"category_id == ${requested_category}")

    videosOfCategory1.show(10, truncate = false)
    println(videosOfCategory1.count())
    videosOfCategory1.foreach(x => {
      println(x.getAs[String]("title"))
    })
  }

  def readFiles(spark: SparkSession): (DataFrame, DataFrame) = {
    try {
      val df = new JsonReader(spark).read("data/CA_category_id.json")

      val df2 = new CsvReader(spark).read("data/CAvideos.csv")
      (df, df2)
    }
    catch {
      case e: FileNotFoundException => {
        println("The requested file surely does not exist")
        exit(1)
      }
    }
  }
}
