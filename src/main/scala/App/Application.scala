package App

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions.row_number
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

//    getVideosFromCategory(dfVideos , dfCategories.collectAsList().get(0).getAs[String]("id")).show(10)
//    meanDislikesPerCategory(dfVideos, dfCategories)
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

  def getVideosFromCategory(dfVideos : DataFrame, category_id : String ): DataFrame = {
    println(s"requested categ : $category_id")
    val videosOfCategory1 = dfVideos.filter(s"category_id == ${category_id}")


    println(s"filtered ${videosOfCategory1.count()} videos")
//    videosOfCategory1.foreach(x => {
//      println(x.getAs[String]("title"))
//    })

    videosOfCategory1
  }

  def meanDislikesPerCategory(dfVideos : DataFrame, dfCategories : DataFrame): DataFrame = {
    val dfVidsSorted = dfVideos.groupBy("category_id").mean("dislikes")

    dfVidsSorted.show(truncate = false , numRows = 20)
    dfVidsSorted
  }
}
