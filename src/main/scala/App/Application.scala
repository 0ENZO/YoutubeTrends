package App

import org.apache.curator.shaded.com.google.common.math.DoubleMath.mean
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.expressions.codegen.CodeFormatter.format
import org.apache.spark.sql.functions.{asc, col, desc, format_number, lit, translate}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
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
    val LANGUAGE = "FR"
    val (dfCategories, dfVideos) = readFiles(spark, LANGUAGE)

    println("dfCategories")
    dfCategories.show(1, truncate = false)

    println("dfVideos")
    dfVideos.show(1, truncate = false)

    getVideosFromCategory(dfVideos , dfCategories.collectAsList().get(0).getAs[String]("id")).show(10)
    getMeanLikesDislikesPerXX(dfVideos).show(false)
    mergeData(dfVideos, dfCategories).show(false)

    getBestRatioPerXX(dfVideos , orderAsc = true).show(false)
  }

  def readFiles(spark: SparkSession , lang : String): (DataFrame, DataFrame) = {
    try {
      val df = new JsonReader(spark).read("data/category_id.json")

      val df2 = new CsvReader(spark).read("data/%svideos.csv".format(lang))
      (df, df2)
    }
    catch {
      case _: FileNotFoundException =>
        println("The requested file surely does not exist")
        exit(1)
    }
  }

  def mergeData(dfVideos : DataFrame, dfCategories : DataFrame): DataFrame = {
    dfVideos
      .join(
        dfCategories,
        dfVideos("category_id") === dfCategories("id")
      )
//      .withColumn("snippet",lit(functions.split(col("snippet").cast("String"), "[A-z &/0-9\\-]*,").getItem(1))).as("categorie")
      .withColumn("category",
        functions.reverse(
          functions.split(
            translate(col("snippet").cast("String") , "{} " , "\0\0\0" ),
            ","
          )
        ).getItem(0)
      )
      .drop("etag" , "kind", "id","snippet", "id_category")
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

  def getMeanLikesDislikesPerXX(dfVideos : DataFrame, columnToGroup : String = "category_id"): DataFrame = {
    val dfLikes = dfVideos
      .withColumnRenamed(columnToGroup , columnToGroup + "_tmp")
      .groupBy(columnToGroup + "_tmp")
      .mean("dislikes").as("Moyenne de dislikes").na.drop()

    val dfDislikes = dfVideos.groupBy(columnToGroup)
      .mean("likes").as("Moyenne de likes").na.drop()

    dfLikes.show()
    dfDislikes.show()


    dfLikes
      .join(
      dfDislikes,
        dfDislikes(columnToGroup) === dfLikes(columnToGroup + "_tmp")
    ).drop(columnToGroup + "_tmp")
  }

  def getBestRatioPerXX(dfVideos : DataFrame , columnToGroup : String = "category_id" , orderAsc : Boolean = false): DataFrame = {
    val newColumn = "ratio UP/DOWN"
    getMeanLikesDislikesPerXX(dfVideos, columnToGroup)
      .withColumn(newColumn , format_number(col("Moyenne de likes.avg(likes)")/(col("Moyenne de dislikes.avg(dislikes)")+0.01), 2).cast("Int"))
    .sort( if(orderAsc) asc(newColumn) else{ desc(newColumn)} )
  }
}
