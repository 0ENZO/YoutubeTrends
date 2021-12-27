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

    getVideosFromCategory(dfVideos , dfCategories.first()).show(10)
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

  def getVideosFromCategory(dfVideos : DataFrame, category : Row ): DataFrame = {
    val category_id = category.getAs[String]("id")
    println(s"requested categ : $category_id")
    val videosOfCategory1 = dfVideos.filter(s"category_id == ${category_id}")


    println(s"filtered ${videosOfCategory1.count()} videos")
//    videosOfCategory1.foreach(x => {
//      println(x.getAs[String]("title"))
//    })

    videosOfCategory1
  }
}
