package App

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import readers.{CsvReader, JsonReader}

import java.io.{File, FileNotFoundException}
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

    val lang_code_to_continent = Map[String, String](
//      "FR" -> "Europe",
      "DE" -> "Europe",
      "GB" -> "Europe",
      "CA" -> "North_America",
      "US" -> "North_America",
      "MX" -> "North_America",
      "IN" -> "Asia",
      "JP" -> "Asia",
      "KR" -> "Asia",
      "RU" -> "Russia"
    )

    val dfCategories = readCategoriesFile(spark)
    val globalVideosDf = getGlobalVideosDf(spark, lang_code_to_continent)
    // globalVideosDf.filter(row => row != globalVideosDf.first())
    println(s"Dataframe de ${globalVideosDf.count()} videos")

    println("dfCategories")
    dfCategories.show(1, truncate = false)

    println("globalVideosDf")
    globalVideosDf.show(1, truncate = false)

    //     getMostTrendingChannels(FR_df)

    val artist = "GMK"
    println(s"getTotalViewsPerCategoryForSpecificChannel $artist")
    getTotalViewsPerCategoryForSpecificChannel(spark, globalVideosDf, artist).show()

    val yearRequested = "2018"
    println(s"getMostWatchedChannelsForSpecificYear $yearRequested")
    getMostWatchedChannelsForSpecificYear(globalVideosDf, yearRequested).show()

    println("getMostWatchedCategoryForEachYear")
    getMostWatchedCategoryForEachYear(spark, globalVideosDf).show()
    //     getMostTrendingsVideos(FR_df).show()

    val requestedCategory = "1"
    println(s"getVideosFromCategory  : $requestedCategory")
    getVideosFromCategory(globalVideosDf, requestedCategory).show(1)

    println("mergeVideosWithCategories")
    mergeVideosWithCategories(globalVideosDf, dfCategories).show(1)

    println("getMeanLikesDislikesPerXX")
    getMeanLikesDislikesPerXX(globalVideosDf).show()

    println("getBestRatioPerXX")
    getBestRatioPerXX(globalVideosDf , orderAsc = true).show()
  }

  def readVideosFile(spark: SparkSession, lang: String): (DataFrame) = {
    try {
      val df = new CsvReader(spark).read(s"data/${lang}videos.csv")
      df
    }
    catch {
      case _: FileNotFoundException =>
        println("The requested file surely does not exist")
        exit(1)
    }
  }

  def readCategoriesFile(spark: SparkSession): DataFrame = {
    try {
      val df = new JsonReader(spark).read("data/category_id.json")

      //      val df2 = new CsvReader(spark).read("data/%svideos.csv".format(lang))
      df
    }
    catch {
      case _: FileNotFoundException =>
        println("The requested file surely does not exist")
        exit(1)
    }
  }

  def mergeVideosWithCategories(dfVideos: DataFrame, dfCategories: DataFrame): DataFrame = {
    dfVideos
      .join(
        dfCategories,
        dfVideos("category_id") === dfCategories("id")
      )
      .withColumn("category",
        functions.reverse(
          functions.split(
            translate(col("snippet").cast("String"), "{} ", "\0\0\0"),
            ","
          )
        ).getItem(0)
      )
      .drop("etag", "kind", "id", "snippet", "id_category")
  }

  def getVideosFromCategory(dfVideos: DataFrame, category_id: String): DataFrame = {
    val videosOfCategory1 = dfVideos.filter(s"category_id == ${category_id}")

    println(s"filtered ${videosOfCategory1.count()} videos")
    //    videosOfCategory1.foreach(x => {
    //      println(x.getAs[String]("title"))
    //    })
    videosOfCategory1
  }

  def getMeanLikesDislikesPerXX(dfVideos: DataFrame, columnToGroup: String = "category_id"): DataFrame = {
    val dfLikes = dfVideos
      .withColumnRenamed(columnToGroup, columnToGroup + "_tmp")
      .groupBy(columnToGroup + "_tmp")
      .mean(col("dislikes").toString()).as("Moyenne de dislikes").na.drop()

    val dfDislikes = dfVideos.groupBy(columnToGroup)
      .mean("likes").as("Moyenne de likes").na.drop()

    dfLikes
      .join(
        dfDislikes,
        dfDislikes(columnToGroup) === dfLikes(columnToGroup + "_tmp")
      ).drop(columnToGroup + "_tmp")
  }

  def getBestRatioPerXX(dfVideos: DataFrame, columnToGroup: String = "category_id", orderAsc: Boolean = false): DataFrame = {
    val newColumn = "ratio UP/DOWN"
    getMeanLikesDislikesPerXX(dfVideos, columnToGroup)
      .withColumn(newColumn, format_number(col("Moyenne de likes.avg(likes)") / (col("Moyenne de dislikes.avg(dislikes)") + 0.01), 2).cast("Int"))
      .sort(if (orderAsc) asc(newColumn) else {
        desc(newColumn)
      })
  }

  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

//  def createEmptyDF(spark: SparkSession): DataFrame = {
//    import spark.implicits._
//
//    val emptyData = Seq(("", "", "", "", "", "", "", "", "", "", "", "", true, true, true, "", "", ""))
//    val df = emptyData.toDF(
//      "video_id", "trending_date", "title", "channel_title", "category_id", "publish_time",
//      "tags", "views", "likes", "dislikes", "comment_count", "thumbnail_link", "comments_disabled",
//      "ratings_disabled", "video_error_or_removed", "description", "lang_code", "continent")
//
//    df.filter($"video_id" === "" )
//  }

  def getGlobalVideosDf(spark: SparkSession, lang_code_to_continent: Map[String, String]): DataFrame = {
    val x = getListOfFiles(s"data/")
//    var main_df = createEmptyDF(spark)
    var main_df = new CsvReader(spark).read(s"data/CAvideos.csv")
      .withColumn("lang_code", lit("FR"))
      .withColumn("continent", lit("Europe"))


    x.foreach(e =>
      if (e.toString.endsWith("v")) { // .csv
        val lang_code = e.toString.substring(5, 7)
        if(lang_code != "FR") {
          println(s"Fichier $lang_code en cours..")
          val df = new CsvReader(spark).read(s"data/${lang_code}videos.csv")
          val df2 = df
            .withColumn("lang_code", lit(lang_code))
            .withColumn("continent", lit(lang_code_to_continent(lang_code)))

          main_df = main_df.union(df2)
        }
      })
    //val final_df = main_df.filter(row => row != main_df.first())
    main_df
  }

  /*
  def getMostTrendingChannels(dfVideos : DataFrame): Unit = {
    val df = dfVideos
      .groupBy("channel_title")
      .agg(sum("views")).as("total_views")
      .orderBy("total_views")
      .show(15)

    //val df = dfVideos.agg(sum("views").as("total_views"), sum("col2").as("sum_col2"), ...).first
  }*/

  def getTotalViewsPerCategoryForSpecificChannel(spark: SparkSession, dfVideos: DataFrame, yt_channel_title: String): DataFrame = {
    import spark.implicits._

    val df = dfVideos
      .filter($"channel_title" === yt_channel_title)
      .groupBy("channel_title", "category_id")
      .agg(sum($"views").as("total_views"), count($"category_id").as("cpt"))
      .select(concat_ws("", $"category_id", lit(" ("), $"cpt", lit(")")).as("Nombre de vues par catégories"), $"total_views")

    df
  }

  def getMostWatchedChannelsForSpecificYear(dfVideos: DataFrame, specific_year: String): DataFrame = {

    dfVideos
      .filter(year(col("publish_time")).geq(lit(specific_year)))
      .groupBy("channel_title")
      .agg(sum("views").as("total_views"))
      .sort(desc("total_views"))
  }

  def getMostWatchedCategoryForEachYear(spark: SparkSession, dfVideos: DataFrame): DataFrame = {
    import spark.implicits._

    val df = dfVideos
      .na.drop()
      .groupBy(year(col("publish_time")).as("year"), col("category_id"))
      .agg(sum("views").as("total_views"))
      .sort(year(col("publish_time")))

    val windowDept = Window.partitionBy(col("year")).orderBy(col("total_views").desc)
    df.withColumn("row", row_number.over(windowDept))
      .where($"row" === 1).drop("row")
  }
  //  def getMostTrendingsVideos(dfVideos: DataFrame): Unit ={


  /*
  val df = dfVideos
    .select("title", "channel_title", "category_id", "views", "trending_date", "publish_time")
    .withColumn("publish_time", df("publish_time").cast(DateType))
    //.withColumn("publish_time", date_format(to_date(col("publish_time"),"MM/dd/yyyy"), "yyyyMMdd"))
    //.withColumn("days_diff", datediff(col("trending_date"), col("publish_time" )))
    .sort(desc("days_diff"))
    .show()
  */
  //  }
}

