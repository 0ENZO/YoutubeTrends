package App

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import readers.{CsvReader, JsonReader}

import java.io.FileNotFoundException
import scala.sys.exit

object Application {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

/*       TODO    apres le join de mergeData il faudrait juste récupérer la partie avec le nom de la catégorie et
          jeter les autres trucs genre etag , description... des trucs qui servent pas trop trop je trouve
          après si tu veux faire d'autres traitements t'es 100% libre copain
*/

    val spark = SparkSession
      .builder()
      .appName("TP1")
      .master("local[*]")
      .getOrCreate()
    val LANGUAGE = "FR"
    val (dfCategories, dfVideos) = readFiles(spark, LANGUAGE)

    println("dfCategories")
    dfCategories.show(1, truncate = false)

    println("dfVideos")
    dfVideos.show(1, truncate = false)

    getVideosFromCategory(dfVideos , dfCategories.collectAsList().get(0).getAs[String]("id")).show(10)
    meanDislikesPerCategory(dfVideos, dfCategories)
    mergeData(dfVideos, dfCategories)


  }

  def readFiles(spark: SparkSession , lang : String): (DataFrame, DataFrame) = {
    try {
      val df = new JsonReader(spark).read(s"data/category_id.json")

      val df2 = new CsvReader(spark).read(s"data/${lang}videos.csv")
      (df, df2)
    }
    catch {
      case _: FileNotFoundException =>
        println("The requested file surely does not exist")
        exit(1)
    }
  }

  def mergeData(dfVideos : DataFrame, dfCategories : DataFrame): DataFrame = {
    val test =dfVideos
      .join(
        dfCategories,
        dfVideos("category_id") === dfCategories("id")
      ).toDF()
//      .withColumn("category_id" , col("_2.id"))
//      .drop("_2")
    test.show(false)
    test
  }
  def getVideosFromCategory(dfVideos : DataFrame, category_id : String ): DataFrame = {
    println(s"requested categ : $category_id")
    val videosOfCategory1 = dfVideos.filter(s"category_id == ${category_id}")


    println(s"filtered ${videosOfCategory1.count()} videos")
//    videosOfCategory1.foreach(x => {
//      println(x.getAs[String]("title"))
//    })
    println("AIE !!!! " , dfVideos.agg(functions.max("category_id")).show)
    videosOfCategory1
  }

  def meanDislikesPerCategory(dfVideos : DataFrame, dfCategories : DataFrame): DataFrame = {
    val dfVidsSorted = dfVideos.groupBy("category_id").mean("dislikes").drop()

    dfVidsSorted.show(truncate = false , numRows = 20)
    dfVidsSorted
  }
}
